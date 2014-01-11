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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.util.Map;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Parameter;

import com.addthis.meshy.ChannelMaster;
import com.addthis.meshy.ChannelState;
import com.addthis.meshy.Meshy;
import com.addthis.meshy.MeshyConstants;
import com.addthis.meshy.SourceHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

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
    public StreamSource(ChannelMaster master, String nodeUuid, String fileName, Map<String, String> params, int bufferSize) throws IOException {
        this(master, MeshyConstants.LINK_ALL, nodeUuid, fileName, params, bufferSize);
    }

    /* server-server constructor (used in FileTarget for proxies) */
    public StreamSource(ChannelMaster master, String targetUuid, String nodeUuid, String fileName, Map<String, String> params, int bufferSize) throws IOException {
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
        if (log.isDebugEnabled()) {
            log.debug(this + " new stream=" + stream + " file=" + fileName + " sendBuffer=" + bufferSize + " prefetch=" + FETCH_ON_OPEN);
        }
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(params == null ? StreamService.MODE_START : StreamService.MODE_START_2);
            Bytes.writeString(nodeUuid, out);
            Bytes.writeString(fileName, out);
            Bytes.writeInt(bufferSize, out);
            if (params != null) {
                Bytes.writeInt(params.size(), out);
                for (Map.Entry<String, String> e : params.entrySet()) {
                    Bytes.writeString(e.getKey(), out);
                    Bytes.writeString(e.getValue(), out);
                }
            }
            if (!send(out.toByteArray())) {
                log.warn("Failed to send stream init data for " + this);
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
        return super.toString() + ";fileName=" + fileName + ";nodeUuid=" + nodeUuid + ";more=" + moreRequests + ";recv=" + recvBytes + ";stream=" + (stream != null ? stream.toStatus() : "null");
    }

    public SourceInputStream getInputStream() {
        if (log.isTraceEnabled()) {
            log.trace(this + " get input stream");
        }
        return stream;
    }

    public void requestMoreData() {
        if (log.isTraceEnabled()) {
            log.trace(this + " send request more");
        }
        send(new byte[]{StreamService.MODE_MORE});
        moreRequests++;
    }

    public void requestClose() {
        if (log.isTraceEnabled()) {
            log.trace(this + " send close");
        }
        send(new byte[]{StreamService.MODE_CLOSE});
        sendComplete();
    }

    @Override
    public void receive(int length, ByteBuf buffer) throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(Meshy.getBytes(length, buffer));
        int mode = in.read();
        if (log.isTraceEnabled()) {
            log.trace(this + " recv mode=" + mode + " len=" + length);
        }
        switch (mode) {
            case StreamService.MODE_MORE:
                byte data[] = Bytes.readBytes(in, in.available());
                recvBytes += data.length;
                recvBytes += ChannelState.MESHY_BYTE_OVERHEAD + StreamService.STREAM_BYTE_OVERHEAD;
                stream.feed(data);
                break;
            case StreamService.MODE_CLOSE:
                stream.feed(new byte[0]);
                break;
            case StreamService.MODE_FAIL:
                stream.feed(StreamService.FAIL_BYTES);
                stream.feed(Bytes.readFully(in));
                break;
            default:
                log.warn("source unknown mode: " + mode);
                break;
        }
    }

    @Override
    public void receiveComplete() throws Exception {
        if (log.isTraceEnabled()) {
            log.trace(this + " recv send complete");
        }
        sendComplete();
    }

    @Override
    public void channelClosed() {
        if (stream != null) {
            stream.feed(StreamService.FAIL_BYTES);
            stream.feed(Bytes.toBytes(StreamService.ERROR_CHANNEL_LOST));
        }
    }
}
