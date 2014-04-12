/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.addthis.meshy.service.stream;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;

import java.net.SocketTimeoutException;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.basis.util.Parameter;

import com.addthis.meshy.ChannelState;
import com.addthis.meshy.util.ByteBufs;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;

import org.slf4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

/**
 * creates an input stream from a blocking finderQueue of byte[]
 * <p/>
 * This class is not thread safe.  It is assumed that only one consumer is reading from this stream
 * at any given time.  It is possible for multiple threads to consume from the same input stream as long
 * as it is guaranteed that only one thread is reading from the stream at any point in time.
 */
public class SourceInputStream extends InputStream {

    /* denominator used to decide when to request more bytes */
    private static final int REFILL_FACTOR = Parameter.intValue("meshy.refill.factor", 2);
    /* max time to wait in seconds for a read response before timing out and closing stream */
    private static final int MAX_READ_WAIT = Parameter.intValue("meshy.stream.timeout", 0) * 1000;

    private static final Logger log = StreamService.log;

    private final AtomicInteger expectingBytes = new AtomicInteger(0);
    private final LinkedBlockingDeque<ByteBuf> deque = new LinkedBlockingDeque<>();
    private final StreamSource source;
    private final int maxBufferSize;
    private final int refillThreshold;
    private ByteBufInputStream current;
    private ByteBuf currentData;
    private boolean done = false;
    private boolean primed = false;

    /* metrics */
    private final Histogram dequeSizeHisto = Metrics.newHistogram(getClass(), "dequeSizeHisto");
    private final Histogram basHisto = Metrics.newHistogram(getClass(), "basHisto");
    private final Counter requestMoreData = Metrics.newCounter(getClass(), "requestMoreData");
    private final Timer dequePollTimer = Metrics.newTimer(getClass(), "dequeTimer");


    @Override
    public String toString() {
        return "SIS(" + source + ") " + toStatus();
    }

    String toStatus() {
        return "{expect=" + expectingBytes + " max=" + maxBufferSize + " q=" + deque.size() + " done=" + done + "}";
    }

    @Override
    protected void finalize() {
        close();
    }

    SourceInputStream(final StreamSource source, final int maxBufferSize) {
        this.source = source;
        this.maxBufferSize = maxBufferSize;
        this.refillThreshold = maxBufferSize / REFILL_FACTOR;
    }

    public int getBufferSize() {
        return maxBufferSize;
    }

    void feed(ByteBuf data) {
        try {
            if (log.isTraceEnabled()) {
                log.trace("{} feed={}", this, data.readableBytes());
            }
            basHisto.update(data.readableBytes());
            deque.put(data);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private boolean fill(boolean blocking) throws IOException {
        return fill(blocking, -1, null);
    }

    /**
     * Blocking or non blocking (depending on how the source was initialized) call to fill  buffer.
     * <p/>
     * If blocking the call will wait until data is available on deque before
     * returning if the current buffer is null or empty.
     * <p/>
     * If non-blocking the call will return when data is available or the time limit (wait) has been
     * reached.  Callers may not assume that a false return from this method means the end of stream
     * has been reached in async mode.
     *
     * @return true if meshy data is available
     * @throws java.io.IOException if remote error
     */
    private boolean fill(boolean blocking, long wait, TimeUnit timeUnit) throws IOException {
        dequeSizeHisto.update(deque.size());
        if (done) {
            return false;
        }
        if ((blocking && (current == null || current.available() == 0)) || (!blocking && currentData == null)) {
            if (log.isTraceEnabled()) {
                log.trace("{} fill c={}", this, current != null ? current.available() : "empty");
            }
            ByteBuf data = null;
            try {
                if (!primed) {
                    requestMoreData();
                    primed = true;
                }
                if (blocking) {
                    if (log.isTraceEnabled()) {
                        log.trace("{} fill from finderQueue={} wait={}", this, deque.size(), MAX_READ_WAIT);
                    }
                    long startTime = System.nanoTime();
                    data = MAX_READ_WAIT > 0 ? deque.poll(MAX_READ_WAIT, TimeUnit.MILLISECONDS) : deque.take();
                    dequePollTimer.update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
                    if (data == null) {
                        /* important that we throw SocketTimeoutException so that SourceTracker does not mark this file "dead" */
                        throw new SocketTimeoutException(this + " timeout waiting for fill()");
                    }
                } else {
                    if (wait > 0) {
                        data = deque.poll(wait, timeUnit);
                    } else {
                        data = deque.poll();
                    }
                    if (data == null) {
                        return false;
                    }
                }

                int sizeIncludingOverhead = data.readableBytes() + ChannelState.MESHY_BYTE_OVERHEAD
                                            + StreamService.STREAM_BYTE_OVERHEAD;

                // returns previous value
                int oldExpectingBytes = expectingBytes.getAndAdd(-sizeIncludingOverhead);
                // if prior to our update, we were above the threshold
                if (oldExpectingBytes >= refillThreshold) {
                    int updatedExpectingBytes = oldExpectingBytes - sizeIncludingOverhead;
                    // and without respect to the current value, we know our update moved it below
                    if (updatedExpectingBytes < refillThreshold) {
                        // for pathologically large chunks, try to recover the buffer state.
                        // this should usually evaluate to '0 + 1'
                        int overflowRecoverCount = (sizeIncludingOverhead / maxBufferSize) + 1;
                        if (overflowRecoverCount != 1) {
                            log.warn("Sending {} sendMore requests due to pathologically large chunk of size {}",
                                    overflowRecoverCount, sizeIncludingOverhead);
                        }
                        requestMoreData(overflowRecoverCount);
                    }
                }
                log.trace("{} fill take={}", this, data.readableBytes());
            } catch (InterruptedException ex) {
                log.warn("{} close on stream service interrupted", this);
                close();
                /* important that we throw InterruptedIOException so that SourceTracker does not mark this file "dead" */
                throw new InterruptedIOException("stream interrupted");
            } catch (Exception ex) {
                log.warn("{} close on error", this, ex);
                close();
                throw new IOException(ex);
            }
            if (data == StreamService.FAIL_BYTES) {
                close();
                ByteBuf error = deque.poll();
                String errorMessage;
                errorMessage = error == null ? "no failure message available." : ByteBufs.toString(error);
                throw new IOException(errorMessage);
            } else if (data.readableBytes() == 0) {
                log.trace("{} fill exit on 0 bytes", this);
                currentData = data;
                done = true;
                return false;
            }
            if (blocking) {
                current = new ByteBufInputStream(data);
            } else {
                currentData = data;
            }
            return true;
        }
        return true;
    }

    void requestMoreData() {
        requestMoreData(1);
    }

    void requestMoreData(int times) {
        expectingBytes.addAndGet(maxBufferSize * times);
        for (int i = 0; i < times; i++) {
            requestMoreData.inc();
            source.requestMoreData();
        }
    }

    /**
     * Polls the deque for available data.
     *
     * @return - a byte array if data is available or null if no data is currently available.
     * @throws java.io.IOException
     */
    public ByteBuf poll() throws IOException {
        return poll(-1, null);
    }

    /**
     * Polls the deque for available data.
     *
     * @param wait     - the amount of time to wait before returning null in the case that no data is available yet
     * @param timeUnit - the time unit for wait
     * @return - a byte array if data is available or null if no data is currently available.
     * @throws java.io.IOException
     */
    public ByteBuf poll(long wait, TimeUnit timeUnit) throws IOException {
        // response from fill is ignored because in async mode the call is ignored
        fill(false, wait, timeUnit);
        ByteBuf data = currentData;
        currentData = null;
        return data;
    }

    @Override
    public int read() throws IOException {
        if (fill(true)) {
            return current.read();
        } else {
            return -1;
        }
    }

    @Override
    public int read(byte[] buf) throws IOException {
        return read(buf, 0, buf.length);
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        if (fill(true)) {
            return current.read(buf, off, len);
        } else {
            return -1;
        }
    }

    @Override
    public int available() throws IOException {
        if (current != null) {
            return current.available();
        }
        ByteBuf peek = deque.peek();
        /* length 0 is valid for EOF packets, so returning 1 is wrong in that case */
        return peek != null ? peek.readableBytes() : 0;
    }

    @Override
    public void close() {
        done = true;
        source.requestClose();
    }

    public boolean isEOF() {
        return done;
    }
}
