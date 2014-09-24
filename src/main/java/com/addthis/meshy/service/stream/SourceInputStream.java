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

import javax.annotation.concurrent.NotThreadSafe;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;

import java.net.SocketTimeoutException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Parameter;

import com.google.common.base.Throwables;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;

import org.slf4j.Logger;

@NotThreadSafe
public class SourceInputStream extends InputStream {

    /* max time to wait in seconds for a read response before timing out and closing stream */
    private static final int MAX_READ_WAIT = Parameter.intValue("meshy.stream.timeout", 0) * 1000;

    private static final Logger log = StreamService.log;

    private final StreamSource source;
    private final BlockingQueue<byte[]> messageQueue;

    private ByteArrayInputStream current;
    private byte[] currentData;
    private boolean done = false;

    /* metrics */
    private static final Timer dequePollTimer = Metrics.newTimer(SourceInputStream.class, "dequeTimer");

    SourceInputStream(StreamSource source) {
        this.source = source;
        this.messageQueue = source.getMessageQueue();
    }

    private boolean fill(boolean blocking) throws IOException {
        return fill(blocking, -1, null);
    }

    /**
     * Blocking or non blocking (depending on how the source was initialized) call to fill  buffer.
     * <p/>
     * If blocking the call will wait until data is available on messageQueue before
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
        if (done) {
            return false;
        }
        if ((blocking && (current == null || current.available() == 0)) || (!blocking && currentData == null)) {
            if (log.isTraceEnabled()) {
                log.trace("{} fill c={}", this, current != null ? current.available() : "empty");
            }
            byte[] data = null;
            try {
                if (blocking) {
                    if (log.isTraceEnabled()) {
                        log.trace("{} fill from finderQueue={} wait={}", this, messageQueue.size(), MAX_READ_WAIT);
                    }
                    long startTime = System.nanoTime();
                    data = MAX_READ_WAIT > 0 ? messageQueue.poll(MAX_READ_WAIT, TimeUnit.MILLISECONDS) : messageQueue.take();
                    dequePollTimer.update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
                    if (data == null) {
                        /* important that we throw SocketTimeoutException so that SourceTracker does not mark this file "dead" */
                        throw new SocketTimeoutException(this + " timeout waiting for fill()");
                    }
                } else {
                    if (wait > 0) {
                        data = messageQueue.poll(wait, timeUnit);
                    } else {
                        data = messageQueue.poll();
                    }
                    if (data == null) {
                        return false;
                    }
                }
                source.performBufferAccounting(data);
                source.throwIfErrorSignal(data);
                if (source.isCloseSignal(data)) {
                    log.trace("{} fill exit on 0 bytes", this);
                    currentData = data;
                    close();
                    return false;
                }
                log.trace("{} fill take={}", this, data.length);
            } catch (InterruptedException ex) {
                log.warn("{} close on stream service interrupted", this);
                close();
                /* important that we throw InterruptedIOException so that SourceTracker does not mark this file "dead" */
                throw new InterruptedIOException("stream interrupted");
            } catch (IOException | RuntimeException ex) {
                log.warn("{} close on error", this, ex);
                close();
                Throwables.propagateIfInstanceOf(ex, IOException.class);
                throw ex;
            }
            if (blocking) {
                current = new ByteArrayInputStream(data);
            } else {
                currentData = data;
            }
            return true;
        }
        return true;
    }

    /**
     * Polls the messageQueue for available data.
     *
     * @return - a byte array if data is available or null if no data is currently available.
     * @throws java.io.IOException
     */
    public byte[] poll() throws IOException {
        return poll(-1, null);
    }

    /**
     * Polls the messageQueue for available data.
     *
     * @param wait     - the amount of time to wait before returning null in the case that no data is available yet
     * @param timeUnit - the time unit for wait
     * @return - a byte array if data is available or null if no data is currently available.
     * @throws java.io.IOException
     */
    public byte[] poll(long wait, TimeUnit timeUnit) throws IOException {
        // response from fill is ignored because in async mode the call is ignored
        fill(false, wait, timeUnit);
        byte[] data = currentData;
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
    public int available() {
        if (current != null) {
            return current.available();
        }
        byte[] peek = messageQueue.peek();
        /* length 0 is valid for EOF packets, so returning 1 is wrong in that case */
        return peek != null ? peek.length : 0;
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
