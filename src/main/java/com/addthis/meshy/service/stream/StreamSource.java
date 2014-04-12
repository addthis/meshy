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

import java.util.Map;

import com.addthis.basis.util.Parameter;

import com.addthis.meshy.ChannelMaster;
import com.addthis.meshy.ChannelState;
import com.addthis.meshy.MeshyConstants;
import com.addthis.meshy.SourceHandler;
import com.addthis.meshy.util.ByteBufs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class StreamSource extends SourceHandler {

    /* max client read-ahead mem cache */
    private static final int DEFAULT_MAX_SEND = Parameter.intValue("meshy.stream.buffer", 1) * 1024 * 1024;
    /* pre-fetch stream on open */
    private static final boolean FETCH_ON_OPEN = Parameter.boolValue("meshy.stream.prefetch", false);

    protected static final Logger log = LoggerFactory.getLogger(StreamSource.class);

    private final SourceInputStream stream;
    private final String fileName;
    private final String nodeUuid;
    private int moreRequests = 0;
    private int recvBytes = 0;

    /* client-server constructor */
    public StreamSource(ChannelMaster master, String nodeUuid, String fileName, int bufferSize) throws IOException {
        this(master, MeshyConstants.LINK_ALL, nodeUuid, fileName, null, bufferSize);
    }

    /* client-server constructor */
    public StreamSource(ChannelMaster master, String nodeUuid, String fileName,
            Map<String, String> params, int bufferSize) throws IOException {
        this(master, MeshyConstants.LINK_ALL, nodeUuid, fileName, params, bufferSize);
    }

    /* server-server constructor (used in FileTarget for proxies) */
    public StreamSource(ChannelMaster master, String targetUuid, String nodeUuid, String fileName,
            Map<String, String> params, int bufferSize) throws IOException {
        super(master, StreamTarget.class, targetUuid);
        this.fileName = fileName;
        this.nodeUuid = nodeUuid;
        if (bufferSize <= 0) {
            bufferSize = DEFAULT_MAX_SEND;
        }
          /* stream only needed for client-server mode */
        // using == instead of .equals because the objects should be the same object and
        // doing this is faster than .equals
        if (targetUuid == MeshyConstants.LINK_ALL || targetUuid == nodeUuid) {
            stream = new SourceInputStream(this, bufferSize);
        } else {
            stream = null;
        }
        log.debug("{} new stream={} file={} sendBuffer={} prefetch={}",
                this, stream, fileName, bufferSize, FETCH_ON_OPEN);
        try {
            ByteBuf out = ByteBufs.quickAlloc();
            out.writeByte(params == null ? StreamService.MODE_START : StreamService.MODE_START_2);
            ByteBufs.writeString(nodeUuid, out);
            ByteBufs.writeString(fileName, out);
            out.writeInt(bufferSize);
            if (params != null) {
                out.writeInt(params.size());
                for (Map.Entry<String, String> e : params.entrySet()) {
                    ByteBufs.writeString(e.getKey(), out);
                    ByteBufs.writeString(e.getValue(), out);
                }
            }
            if (!send(out)) {
                log.warn("Failed to send stream init data for {}", this);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        if (stream != null && FETCH_ON_OPEN) {
            stream.requestMoreData();
        }
    }

    @Override
    public String toString() {
        return super.toString() + ";fileName=" + fileName + ";nodeUuid=" + nodeUuid + ";more=" + moreRequests +
               ";recv=" + recvBytes + ";stream=" + (stream != null ? stream.toStatus() : "null");
    }

    public SourceInputStream getInputStream() {
        log.trace("{} get input stream", this);
        return stream;
    }

    public void requestMoreData() {
        log.trace("{} send request more", this);
        send(Unpooled.wrappedBuffer(new byte[]{StreamService.MODE_MORE}));
        moreRequests++;
    }

    public void requestClose() {
        log.trace("{} send close", this);
        send(Unpooled.wrappedBuffer(new byte[]{StreamService.MODE_CLOSE}));
        sendComplete();
    }

    @Override
    public void receive(ChannelState state, ByteBuf in) throws Exception {
        int mode = in.readByte();
        log.trace("{} recv mode={}", this, mode);
        switch (mode) {
            case StreamService.MODE_MORE:
                recvBytes += in.readableBytes();
                recvBytes += ChannelState.MESHY_BYTE_OVERHEAD + StreamService.STREAM_BYTE_OVERHEAD;
                stream.feed(in);
                break;
            case StreamService.MODE_CLOSE:
                stream.feed(StreamService.CLOSE_BYTES);
                break;
            case StreamService.MODE_FAIL:
                stream.feed(StreamService.FAIL_BYTES);
                stream.feed(in);
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
    }

    @Override
    public void channelClosed(ChannelState state) {
        if (stream != null) {
            stream.feed(StreamService.FAIL_BYTES);
            stream.feed(ByteBufs.fromString(StreamService.ERROR_CHANNEL_LOST));
        }
    }
}
