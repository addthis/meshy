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
package com.addthis.meshy;

import java.net.InetSocketAddress;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.addthis.basis.util.Parameter;

import com.google.common.base.Objects;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * TODO implement pinger to teardown "dead" peers
 */
public class ChannelState extends SimpleChannelHandler {

    private static final Logger log = LoggerFactory.getLogger(ChannelState.class);
    private static final int excessiveTargets = Parameter.intValue("meshy.channel.report.targets", 2000);
    private static final int excessiveSources = Parameter.intValue("meshy.channel.report.sources", 2000);

    protected static final BufferAllocator bufferFactory = new BufferAllocator();

    public static final int MESHY_BYTE_OVERHEAD = 4 + 4 + 4;

    protected static enum READMODE {
        ReadType, ReadSession, ReadLength, ReadData
    }

    public static ChannelBuffer allocateSendBuffer(int type, int session, byte[] data) {
        return allocateSendBuffer(type, session, data, 0, data.length);
    }

    public static ChannelBuffer allocateSendBuffer(int type, int session, byte[] data, int off, int len) {
        ChannelBuffer sendBuffer = allocateSendBuffer(type, session, len);
        sendBuffer.writeBytes(data, off, len);
        return sendBuffer;
    }

    public static ChannelBuffer allocateSendBuffer(int type, int session, int length) {
        ChannelBuffer sendBuffer = bufferFactory.allocateBuffer(MESHY_BYTE_OVERHEAD + length);
        sendBuffer.writeInt(type);
        sendBuffer.writeInt(session);
        sendBuffer.writeInt(length);
        return sendBuffer;
    }

    public static ChannelBuffer allocateSendBuffer(int type, int session, ChannelBuffer from, int length) {
        ChannelBuffer sendBuffer = bufferFactory.allocateBuffer(MESHY_BYTE_OVERHEAD + length);
        sendBuffer.writeInt(type);
        sendBuffer.writeInt(session);
        sendBuffer.writeInt(length);
        from.readBytes(sendBuffer, length);
        return sendBuffer;
    }

    public static void returnSendBuffer(ChannelBuffer buffer) {
        bufferFactory.returnBuffer(buffer);
    }

    private final ConcurrentHashMap<Integer, SessionHandler> targetHandlers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, SourceHandler> sourceHandlers = new ConcurrentHashMap<>();
    private final ChannelMaster master;
    private final Channel channel;

    private ChannelBuffer buffer = bufferFactory.allocateBuffer(16384);
    private READMODE mode = READMODE.ReadType;
    // last session id for which a handler was created on this channel. should always be > than the last
    private int lastCreatedSession = -1;
    private int type;
    private int session;
    private int length;
    private String name;
    //    private int remotePort;
    private InetSocketAddress remoteAddress;

    ChannelState(ChannelMaster master, Channel channel) {
        super();
        this.master = master;
        this.channel = channel;
    }

    protected Objects.ToStringHelper toStringHelper() {
        return Objects.toStringHelper(this)
                .add("targets", targetHandlers.size())
                .add("sources", sourceHandlers.size())
                .add("name", name)
                .add("remoteAddress", remoteAddress)
                .add("mode", mode)
                .add("type", type)
                .add("session", session)
                .add("length", length)
                .add("channel", channel)
                .add("master", master.getUUID());
    }

    @Override
    public String toString() {
        return toStringHelper().toString();
    }

    /* helper to debug close() with open sessions/targets */
    protected void debugSessions() {
        if (!targetHandlers.isEmpty()) {
            log.info("{} targets --> {}", this, targetHandlers);
        }
        if (!sourceHandlers.isEmpty()) {
            log.info("{} sources --> {}", this, sourceHandlers);
        }
    }

    public boolean send(final ChannelBuffer sendBuffer, final SendWatcher watcher, final int reportBytes) {
        if (channel.isOpen()) {
            ChannelFuture future = channel.write(sendBuffer);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture ignored) throws Exception {
                    master.sentBytes(reportBytes);
                    returnSendBuffer(sendBuffer);
                    if (watcher != null) {
                        watcher.sendFinished(reportBytes);
                    }
                }
            });
            return true;
        } else {
            if (reportBytes > 0) {
                /**
                 * if bytes == 0 then it's a sendComplete() and
                 * there are plenty of legit cases when a client would
                 * disconnect before sendComplete() makes it back. best
                 * example is StreamService when EOF framing tells the
                 * client we're done before sendComplete() framing does.
                 */
                log.info("{} writing [{}] to dead channel", this, reportBytes);
                if (watcher != null) {
                    /**
                     * for accounting, rate limiting reasons, we have to report these as sent.
                     * no need to report 0 bytes since it's an accounting no-op.
                     */
                    watcher.sendFinished(reportBytes);
                }
            }
            return false;
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        log.debug("{} setName={}", this, name);
        this.name = name;
    }

    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    public void setRemoteAddress(InetSocketAddress addr) {
        this.remoteAddress = addr;
    }

    public Channel getChannel() {
        return channel;
    }

    public InetSocketAddress getChannelRemoteAddress() {
        return channel != null ? (InetSocketAddress) channel.getRemoteAddress() : null;
    }

    public ChannelMaster getChannelMaster() {
        return master;
    }

    public void addSourceHandler(int sessionID, SourceHandler handler) {
        sourceHandlers.put(sessionID, handler);
        if (sourceHandlers.size() >= excessiveSources) {
            log.debug("excessive sources reached: {}", sourceHandlers.size());
            if (log.isTraceEnabled()) {
                debugSessions();
            }
        }
    }

    public void removeHandlerOnComplete(TargetHandler targetHandler) {
        /* ensure target session closure */
        if (targetHandlers.remove(targetHandler.getSessionId()) != null) {
            log.debug("handler lingering on complete: {}", targetHandler);
        }
    }

    public void channelConnected(ChannelStateEvent e) {
        log.debug("{} channel:connect [{}] {}", this, this.hashCode(), e);
    }

    public void channelClosed(ChannelStateEvent e) throws Exception {
        log.debug("{} channel:close [{}] {}", this, this.hashCode(), e);
        for (Map.Entry<Integer, SessionHandler> entry : targetHandlers.entrySet()) {
            entry.getValue().receiveComplete(this, entry.getKey());
        }
        for (Map.Entry<Integer, SourceHandler> entry : sourceHandlers.entrySet()) {
            entry.getValue().receiveComplete(this, entry.getKey());
        }
    }

    public void messageReceived(MessageEvent msg) {
        log.trace("{} recv msg={}", this, msg);
        final ChannelBuffer in = (ChannelBuffer) msg.getMessage();
        master.recvBytes(in.readableBytes());
        if (buffer.writableBytes() >= in.readableBytes()) {
            buffer.writeBytes(in);
        } else {
            ChannelBuffer out = bufferFactory.allocateBuffer(in.readableBytes() + buffer.readableBytes());
            out.writeBytes(buffer);
            out.writeBytes(in);
            returnSendBuffer(buffer);
            buffer = out;
            log.debug("{} recv.reallocate: {}", this, out.writableBytes());
        }
        loop:
        while (true) {
            switch (mode) {
                // zero signifies a reply to a source
                case ReadType:
                    if (buffer.readableBytes() < 4) {
                        break loop;
                    }
                    type = buffer.readInt();
                    mode = READMODE.ReadSession;
                    continue;
                case ReadSession:
                    if (buffer.readableBytes() < 4) {
                        break loop;
                    }
                    session = buffer.readInt();
                    mode = READMODE.ReadLength;
                    continue;
                    // zero length signifies end of session
                case ReadLength:
                    if (buffer.readableBytes() < 4) {
                        break loop;
                    }
                    length = buffer.readInt();
                    mode = READMODE.ReadData;
                    continue;
                case ReadData:
                    int readable = buffer.readableBytes();
                    if (readable < length) {
                        break loop;
                    }
                    SessionHandler handler = null;
                    if (type == MeshyConstants.KEY_RESPONSE) {
                        handler = sourceHandlers.get(session);
                    } else {
                        handler = targetHandlers.get(session);
                        if ((handler == null) && (master instanceof MeshyServer)) {
                            if (session > lastCreatedSession) {
                                handler = master.createHandler(type);
                                lastCreatedSession = session;
                                ((TargetHandler) handler).setContext(((MeshyServer) master), this, session);
                                log.debug("{} createHandler {} session={}", this, handler, session);
                                if (targetHandlers.put(session, handler) != null) {
                                    log.debug("clobbered session {} with {}", session, handler);
                                }
                                if (targetHandlers.size() >= excessiveTargets) {
                                    log.debug("excessive targets reached, current targetHandlers = {}", targetHandlers.size());
                                    if (log.isTraceEnabled()) {
                                        debugSessions();
                                    }
                                }
                            } else {
                                log.debug("Ignoring bad handler creation request for session {} lastSession {}",
                                        session, lastCreatedSession); // happens with fast streams and send-mores
                            }
                        }
                    }
                    if (handler != null) {
                        try {
                            if (length == 0) {
                                handler.receiveComplete(this, session);
                                if (type == MeshyConstants.KEY_RESPONSE) {
                                    sourceHandlers.remove(session);
                                    log.debug("{} dropSession session={}", this, session);
                                } else {
                                    targetHandlers.remove(session);
                                }
                            } else {
                                handler.receive(this, session, length, buffer);
                            }
                        } catch (Exception ex) {
                            log.warn("messageReceived error", ex);
                        }
                    }
                    int read = readable - buffer.readableBytes();
                    if (read < length) {
                        if (handler != null || log.isDebugEnabled()) {
                            log.debug("{} recv type={} handler={} ssn={} did not consume all bytes (read={} of {})",
                                    this, type, handler, session, read, length);
                        }
                        buffer.skipBytes(length - read);
                    }
                    mode = READMODE.ReadType;
                    continue;
                default:
                    throw new RuntimeException("invalid state");
            }
        }
        buffer.discardReadBytes();
    }
}
