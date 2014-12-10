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

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Parameter;

import com.addthis.meshy.ChannelMaster;
import com.addthis.meshy.ChannelState;
import com.addthis.meshy.Meshy;
import com.addthis.meshy.MeshyConstants;
import com.addthis.meshy.SourceHandler;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

public class StreamSource extends SourceHandler {

    protected static final Logger log = LoggerFactory.getLogger(StreamSource.class);

    /* denominator used to decide when to request more bytes */
    private static final int REFILL_FACTOR = Parameter.intValue("meshy.refill.factor", 2);
    /* max client read-ahead mem cache */
    private static final int DEFAULT_MAX_SEND = Parameter.intValue("meshy.stream.buffer", 1) * 1024 * 1024;
    /* pre-fetch stream on open */
    private static final boolean FETCH_ON_OPEN = Parameter.boolValue("meshy.stream.prefetch", false);

    @Nullable private final BlockingQueue<byte[]> messageQueue;

    private final String fileName;
    private final String nodeUuid;
    private final int maxBufferSize;
    private final int refillThreshold;
    private final boolean isProxy;

    private final AtomicLong expectingBytes;
    private final AtomicLong moreRequests;
    private final AtomicLong recvBytes;
    private final AtomicBoolean closeSent;

    @Nullable private volatile String err = StreamService.ERROR_UNKNOWN;

    /* client-server constructor */
    public StreamSource(ChannelMaster master, String nodeUuid, String fileName, int bufferSize) throws IOException {
        this(master, MeshyConstants.LINK_ALL, nodeUuid, fileName, null /* params */, bufferSize);
    }

    /* client-server constructor */
    public StreamSource(ChannelMaster master,
                        String nodeUuid,
                        String fileName,
                        Map<String, String> params,
                        int bufferSize) throws IOException {
        this(master, MeshyConstants.LINK_ALL, nodeUuid, fileName, params, bufferSize);
    }

    /* server-server constructor (used in FileTarget for proxies) */
    public StreamSource(ChannelMaster master,
                        String targetUuid,
                        String nodeUuid,
                        String fileName,
                        @Nullable Map<String, String> params,
                        int bufferSize) {
        super(master, StreamTarget.class, targetUuid);
        this.fileName = fileName;
        this.nodeUuid = nodeUuid;
        if (bufferSize <= 0) {
            this.maxBufferSize = DEFAULT_MAX_SEND;
        } else {
            this.maxBufferSize = bufferSize;
        }
        this.refillThreshold = maxBufferSize / REFILL_FACTOR;
        this.expectingBytes = new AtomicLong();
        this.recvBytes = new AtomicLong();
        this.moreRequests = new AtomicLong();
        this.closeSent = new AtomicBoolean();

        if ((targetUuid == MeshyConstants.LINK_ALL) || (targetUuid == nodeUuid)) {
            this.isProxy = false;
            this.messageQueue = new LinkedBlockingQueue<>();
        } else {
            this.isProxy = true;
            this.messageQueue = null;
        }

        log.debug("{} new file={} sendBuffer={} prefetch={}", this, fileName, maxBufferSize, FETCH_ON_OPEN);

        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(params == null ? StreamService.MODE_START : StreamService.MODE_START_2);
            Bytes.writeString(nodeUuid, out);
            Bytes.writeString(fileName, out);
            Bytes.writeInt(maxBufferSize, out);
            if (params != null) {
                Bytes.writeInt(params.size(), out);
                for (Map.Entry<String, String> e : params.entrySet()) {
                    Bytes.writeString(e.getKey(), out);
                    Bytes.writeString(e.getValue(), out);
                }
            }
            if (!send(out.toByteArray())) {
                log.warn("Failed to send stream init data for {}", this);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        if (!isProxy && FETCH_ON_OPEN) {
            requestMoreData();
        }
    }

    public BlockingQueue<byte[]> getMessageQueue() {
        maybePrime();
        return messageQueue;
    }

    public SourceInputStream getInputStream() {
        return new SourceInputStream(this);
    }

    @Override
    public void receive(ChannelState state, int length, ByteBuf buffer) throws Exception {
        assert messageQueue != null : "must override receive for proxy mode";
        ByteArrayInputStream in = new ByteArrayInputStream(Meshy.getBytes(length, buffer));
        int mode = in.read();
        log.trace("{} recv mode={} len={}", this, mode, length);
        switch (mode) {
            case StreamService.MODE_MORE:
                byte[] data = Bytes.readBytes(in, in.available());
                recvBytes.addAndGet((long) data.length);
                recvBytes.addAndGet(ChannelState.MESHY_BYTE_OVERHEAD + StreamService.STREAM_BYTE_OVERHEAD);
                messageQueue.put(data);
                break;
            case StreamService.MODE_CLOSE:
                messageQueue.put(StreamService.CLOSE_BYTES);
                break;
            case StreamService.MODE_FAIL:
                err = Bytes.toString(Bytes.readFully(in));
                messageQueue.put(StreamService.FAIL_BYTES);
                break;
            default:
                log.warn("source unknown mode: {}", mode);
                break;
        }
    }

    @Override
    public void receiveComplete() throws Exception {
        log.trace("{} recv send complete", this);
        sendComplete();
        // this fail signal should never be reached unless something is wrong
        assert messageQueue != null : "must override receiveComplete for proxy mode";
        messageQueue.put(StreamService.FAIL_BYTES);
    }

    @Override
    public void channelClosed(ChannelState state) {
        if (messageQueue != null) {
            try {
                err = StreamService.ERROR_CHANNEL_LOST;
                messageQueue.put(StreamService.FAIL_BYTES);
            } catch (InterruptedException ie) {
                Throwables.propagate(ie); // unfortunate but maintains api for now
            }
        }
    }

    public void requestClose() {
        if (closeSent.compareAndSet(false, true)) {
            log.trace("{} send close", this);
            send(new byte[]{StreamService.MODE_CLOSE});
            sendComplete();
        } else {
            log.debug("superfluous requestClose call performed");
        }
    }

    public final void performBufferAccounting(byte[] data) {
        int sizeIncludingOverhead = data.length + ChannelState.MESHY_BYTE_OVERHEAD
                                     + StreamService.STREAM_BYTE_OVERHEAD;

        // returns previous value
        long oldExpectingBytes = expectingBytes.getAndAdd(-sizeIncludingOverhead);
        // if prior to our update, we were above the threshold
        if (oldExpectingBytes >= refillThreshold) {
            long updatedExpectingBytes = oldExpectingBytes - sizeIncludingOverhead;
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
        log.trace("{} fill take={}", this, data.length);
    }

    public final boolean isCloseSignal(byte[] data) {
        if ((data.length == 0) && (data != StreamService.FAIL_BYTES)) {
            if (data != StreamService.CLOSE_BYTES) {
                log.debug("size zero application layer data found");
            }
            return true;
        }
        return false;
    }

    public final void throwIfErrorSignal(byte[] data) throws IOException {
        if (data == StreamService.FAIL_BYTES) {
            throw new IOException(err);
        }
    }

    public final void requestMoreData() {
        requestMoreData(1);
    }

    public final void requestMoreData(long times) {
        expectingBytes.addAndGet((long) maxBufferSize * times);
        for (int i = 0; i < times; i++) {
            moreRequests.incrementAndGet();
            requestMoreDataInternal();
        }
    }

    public boolean maybePrime() {
        if (moreRequests.compareAndSet(0, 1)) {
            // avoid double incrementing moreRequests
            expectingBytes.addAndGet((long) maxBufferSize);
            requestMoreDataInternal();
            return true;
        }
        return false;
    }

    private void requestMoreDataInternal() {
        log.trace("{} send request more", this);
        send(new byte[]{StreamService.MODE_MORE});
    }

    @Override public String toString() {
        return Objects.toStringHelper(this)
                      .add("messageQueue.size", (messageQueue == null) ? "(queue is null)" : messageQueue.size())
                      .add("fileName", fileName)
                      .add("nodeUuid", nodeUuid)
                      .add("maxBufferSize", maxBufferSize)
                      .add("refillThreshold", refillThreshold)
                      .add("isProxy", isProxy)
                      .add("expectingBytes", expectingBytes)
                      .add("moreRequests", moreRequests)
                      .add("recvBytes", recvBytes)
                      .add("closeSent", closeSent)
                      .add("err", err)
                      .toString();
    }
}
