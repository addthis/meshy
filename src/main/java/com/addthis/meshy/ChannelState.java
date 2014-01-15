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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;


/**
 * TODO implement pinger to teardown "dead" peers
 */
public class ChannelState {

    private static final Logger log = LoggerFactory.getLogger(ChannelState.class);
    private static final int excessiveTargets = Parameter.intValue("meshy.channel.report.targets", 2000);
    private static final int excessiveSources = Parameter.intValue("meshy.channel.report.sources", 2000);

    public static final int MESHY_BYTE_OVERHEAD = 4 + 4 + 4;

    public static ByteBuf allocateSendBuffer(int type, int session, byte[] data) {
        return allocateSendBuffer(type, session, data, 0, data.length);
    }

    public static ByteBuf allocateSendBuffer(int type, int session, byte[] data, int off, int len) {
        ByteBuf sendBuffer = allocateSendBuffer(type, session, len);
        sendBuffer.writeBytes(data, off, len);
        return sendBuffer;
    }

    public static ByteBuf allocateSendBuffer(int type, int session, int length) {
        ByteBuf sendBuffer = PooledByteBufAllocator.DEFAULT.buffer(MESHY_BYTE_OVERHEAD + length);
        sendBuffer.writeInt(type);
        sendBuffer.writeInt(session);
        sendBuffer.writeInt(length);
        return sendBuffer;
    }

    public static ByteBuf allocateSendBuffer(int type, int session, ByteBuf from, int length) {
        ByteBuf sendBuffer = PooledByteBufAllocator.DEFAULT.buffer(MESHY_BYTE_OVERHEAD + length);
        sendBuffer.writeInt(type);
        sendBuffer.writeInt(session);
        sendBuffer.writeInt(length);
        from.readBytes(sendBuffer, length);
        return sendBuffer;
    }

    private final ConcurrentHashMap<Integer, SessionHandler> targetHandlers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, SourceHandler> sourceHandlers = new ConcurrentHashMap<>();
    private final ChannelMaster master;
    private final Channel channel;

    private String name;
    //    private int remotePort;
    private InetSocketAddress remoteAddress;

    ChannelState(ChannelMaster master, Channel channel) {
        this.master = master;
        this.channel = channel;
    }

    protected Objects.ToStringHelper toStringHelper() {
        return Objects.toStringHelper(this)
                .add("targets", targetHandlers.size())
                .add("sources", sourceHandlers.size())
                .add("name", name)
                .add("remoteAddress", remoteAddress)
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

    public boolean send(final ByteBuf sendBuffer, final SendWatcher watcher, final int reportBytes) {
        if (channel.isOpen()) {
            ChannelFuture future = channel.write(sendBuffer);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture ignored) throws Exception {
                    master.sentBytes(reportBytes);
                    sendBuffer.release();
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
                log.info(this + " writing [" + reportBytes + "] to dead channel");
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
        log.debug(this + " setName=" + name);
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
        return channel != null ? (InetSocketAddress) channel.remoteAddress() : null;
    }

    public ChannelMaster getChannelMaster() {
        return master;
    }

    public void addSourceHandler(int sessionID, SourceHandler handler) {
        sourceHandlers.put(sessionID, handler);
        if (sourceHandlers.size() >= excessiveSources) {
            log.debug("excessive sources reached: " + sourceHandlers.size());
            if (log.isTraceEnabled()) {
                debugSessions();
            }
        }
    }

    public void removeHandlerOnComplete(TargetHandler targetHandler) {
        /* ensure target session closure */
        if (targetHandlers.remove(targetHandler.getSessionId()) != null) {
            log.debug("handler lingering on complete: " + targetHandler);
        }
    }

    public void channelRegistered() {
        log.debug(this + " channel:connect [" + this.hashCode() + "]");
    }

    public void channelClosed() throws Exception {
        log.debug(this + " channel:close [" + this.hashCode() + "]");
        for (Map.Entry<Integer, SessionHandler> entry : targetHandlers.entrySet()) {
            entry.getValue().receiveComplete(this, entry.getKey());
        }
        for (Map.Entry<Integer, SourceHandler> entry : sourceHandlers.entrySet()) {
            entry.getValue().receiveComplete(this, entry.getKey());
        }
    }

    public void channelRead(Object msg) {
        log.trace("{} recv msg={}", this, msg);
        final ByteBuf buffer = (ByteBuf) msg;
        master.recvBytes(buffer.readableBytes());
        // zero signifies a reply to a source
        int type = buffer.readInt();
        int session = buffer.readInt();
        // zero length signifies end of session
        int length = buffer.readInt();
        SessionHandler handler = null;
        if (type == MeshyConstants.KEY_RESPONSE) {
            handler = sourceHandlers.get(session);
        } else {
            handler = targetHandlers.get(session);
            if (handler == null && master instanceof MeshyServer) {
                handler = master.createHandler(type);
                ((TargetHandler) handler).setContext(((MeshyServer) master), this, session);
                if (log.isDebugEnabled()) {
                    log.debug(this + " createHandler " + handler + " session=" + session);
                }
                if (targetHandlers.put(session, handler) != null) {
                    log.debug("clobbered session " + session + " with " + handler);
                }
                if (targetHandlers.size() >= excessiveTargets) {
                    log.debug("excessive targets reached, current targetHandlers = " + targetHandlers.size());
                    if (log.isTraceEnabled()) {
                        debugSessions();
                    }
                }
            }
        }
        if (handler != null) {
            try {
                if (length == 0) {
                    handler.receiveComplete(this, session);
                    if (type == MeshyConstants.KEY_RESPONSE) {
                        sourceHandlers.remove(session);
                        if (log.isDebugEnabled()) {
                            log.debug(this + " dropSession session=" + session);
                        }
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
        int unread = buffer.readableBytes();
        if (unread > 0) {
            if ((handler != null) || log.isDebugEnabled()) {
                log.debug(this + " recv type=" + type + " handler=" + handler + " ssn=" + session + " did not consume all bytes (unread=" + unread + " of " + length + ")");
            }
        }
    }
}
