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

import javax.annotation.Nullable;

import java.net.InetSocketAddress;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.basis.util.Parameter;

import com.addthis.meshy.service.file.FileTarget;

import com.google.common.base.Objects;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.socket.nio.NioSocketChannel;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.Math.max;
import static java.lang.Math.min;


public class ChannelState extends ChannelDuplexHandler {

    private static final Logger log = LoggerFactory.getLogger(ChannelState.class);
    private static final int excessiveTargets = Parameter.intValue("meshy.channel.report.targets", 2000);
    private static final int excessiveSources = Parameter.intValue("meshy.channel.report.sources", 2000);

    static final AtomicInteger writeSleeps = new AtomicInteger(0);
    static final Meter sleepMeter = Metrics.newMeter(FileTarget.class, "writeSleeps", "sleeps", TimeUnit.SECONDS);

    public static final int MESHY_BYTE_OVERHEAD = 4 + 4 + 4;

    protected static enum READMODE {
        ReadType, ReadSession, ReadLength, ReadData
    }

    private final ConcurrentMap<Integer, SessionHandler> targetHandlers = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, SourceHandler> sourceHandlers = new ConcurrentHashMap<>();
    private final ByteBuf buffer;
    private final Meshy meshy;
    private final NioSocketChannel channel;

    // frame parsing state
    private READMODE mode = READMODE.ReadType;
    // last session id for which a handler was created on this channel. should always be > than the last
    private int type;
    private int session;
    private int length;

    // can be concurrently modified by the peering service; guarded by the connected channels sync
    private String name;
    private InetSocketAddress remoteAddress;

    ChannelState(Meshy meshy, NioSocketChannel channel) {
        this.meshy = meshy;
        this.channel = channel;
        this.buffer = channel.alloc().buffer(16384);
    }

    @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            meshy.updateLastEventTime();
            messageReceived((ByteBuf) msg);
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        meshy.updateLastEventTime();
        log.warn("Netty exception caught. Closing channel. ChannelState: {}", this, cause);
        ctx.channel().close();
    }

    @Override public void channelActive(ChannelHandlerContext ctx) {
        meshy.updateLastEventTime();
        channelConnected();
        meshy.channelConnected(ctx.channel(), this);
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        meshy.updateLastEventTime();
        channelClosed();
        meshy.channelClosed(ctx.channel(), this);
    }

    @Override public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        buffer.release();
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
                .add("master", meshy.getUUID());
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
        int attempts = 1;
        while (channel.isActive()) {
            final boolean inEventLoop = channel.eventLoop().inEventLoop();
            // if this thread is the io loop for this channel: make sure it is allowed to do writes
            if (inEventLoop && !channel.isWritable()) {
                channel.unsafe().forceFlush();
            }
            if (channel.isWritable()
                || (attempts >= 500)
                // if this thread is the io loop for this channel: block writes only based on the actual socket queue;
                // channel writability factors in queued writes from other threads that we can't get to until we finish.
                || (inEventLoop && (channel.unsafe().outboundBuffer().size() < estimateMaxQueued(sendBuffer)))) {
                if (attempts >= 500) {
                    log.warn("Forcing write despite channel being backed up to prevent possible distributed deadlock");
                }
                ChannelFuture future = channel.writeAndFlush(sendBuffer);
                future.addListener(ignored -> {
                    meshy.sentBytes(reportBytes);
                    if (watcher != null) {
                        watcher.sendFinished(reportBytes);
                    }
                });
                return true;
            } else {
                if ((attempts % 100) == 0) {
                    ChannelOutboundBuffer outboundBuffer = channel.unsafe().outboundBuffer();
                    if (outboundBuffer != null) {
                        log.info("Sleeping write because channel is unwritable. Attempt: {}, Pending Bytes: {}, State: {}",
                                 attempts, outboundBuffer.totalPendingWriteBytes(), this);
                    }
                }
                sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
                writeSleeps.incrementAndGet();
                sleepMeter.mark();
            }
            attempts += 1;
        }
        if (reportBytes > 0) {
            /**
             * if bytes == 0 then it's a sendComplete() and
             * there are plenty of legit cases when a client would
             * disconnect before sendComplete() makes it back. best
             * example is StreamService when EOF framing tells the
             * client we're done before sendComplete() framing does.
             */
            log.debug("{} writing [{}] to dead channel", this, reportBytes);
            if (watcher != null) {
                /**
                 * for accounting, rate limiting reasons, we have to report these as sent.
                 * no need to report 0 bytes since it's an accounting no-op.
                 */
                watcher.sendFinished(reportBytes);
            }
        }
        sendBuffer.release();
        return false;
    }

    // This estimation is cheap / easy, and should keep pending writes bounded by 2 * high water mark
    private int estimateMaxQueued(ByteBuf sendBuffer) {
        return max(1, min(64, (channel.config().getWriteBufferHighWaterMark() / sendBuffer.readableBytes()) >> 6));
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

    public NioSocketChannel getChannel() {
        return channel;
    }

    @Nullable public InetSocketAddress getChannelRemoteAddress() {
        return channel.remoteAddress();
    }

    public ChannelMaster getChannelMaster() {
        return meshy;
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

    public void channelConnected() {
        log.debug("{} channel:connect [{}]", this, this.hashCode());
    }

    public void channelClosed() throws Exception {
        log.debug("{} channel:close [{}]", this, this.hashCode());
        for (Map.Entry<Integer, SessionHandler> entry : targetHandlers.entrySet()) {
            entry.getValue().receiveComplete(this, entry.getKey());
        }
        for (Map.Entry<Integer, SourceHandler> entry : sourceHandlers.entrySet()) {
            entry.getValue().receiveComplete(this, entry.getKey());
        }
    }

    public void messageReceived(ByteBuf in) {
        log.trace("{} recv msg={}", this, in);
        meshy.recvBytes(in.readableBytes());
        buffer.writeBytes(in);
        in.release();
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
                        if ((handler == null) && (meshy instanceof MeshyServer)) {
                            if (type != MeshyConstants.KEY_EXISTING) {
                                handler = meshy.createHandler(type);
                                ((TargetHandler) handler).setContext((MeshyServer) meshy, this, session);
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
                                log.debug("Ignoring bad handler creation request for session {} type {}",
                                        session, type); // happens with fast streams and send-mores
                            }
                        }
                    }
                    if (handler != null) {
                        if (length == 0) {
                            sessionComplete(handler, type, session);
                        } else {
                            try {
                                handler.receive(this, session, length, buffer);
                            } catch (Exception ex) {
                                log.error("suppressing handler exception during receive; trying receiveComplete", ex);
                                sessionComplete(handler, type, session);
                            }
                        }
                    }
                    int read = readable - buffer.readableBytes();
                    if (read < length) {
                        if ((handler != null) || log.isDebugEnabled()) {
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

    public boolean sessionComplete(SessionHandler handler, int sessionType, int sessionId) {
        boolean wasRemoved;
        if (sessionType == MeshyConstants.KEY_RESPONSE) {
            wasRemoved = sourceHandlers.remove(sessionId) != null;
        } else {
            wasRemoved = targetHandlers.remove(sessionId) != null;
        }
        if (wasRemoved) {
            log.debug("{} sessionComplete type={} session={}", this, sessionType, sessionId);
            try {
                handler.receiveComplete(this, sessionId);
            } catch (Exception ex) {
                log.error("suppressing handler exception during receive complete", ex);
            }
        }
        return wasRemoved;
    }

    public ByteBuf allocateSendBuffer(int type, int session, byte[] data) {
        return allocateSendBuffer(type, session, data, 0, data.length);
    }

    public ByteBuf allocateSendBuffer(int type, int session, byte[] data, int off, int len) {
        ByteBuf sendBuffer = allocateSendBuffer(type, session, len);
        sendBuffer.writeBytes(data, off, len);
        return sendBuffer;
    }

    public ByteBuf allocateSendBuffer(int type, int session, int length) {
        ByteBuf sendBuffer = channel.alloc().buffer(MESHY_BYTE_OVERHEAD + length);
        sendBuffer.writeInt(type);
        sendBuffer.writeInt(session);
        sendBuffer.writeInt(length);
        return sendBuffer;
    }

    public ByteBuf allocateSendBuffer(int type, int session, ByteBuf from, int length) {
        ByteBuf sendBuffer = channel.alloc().buffer(MESHY_BYTE_OVERHEAD + length);
        sendBuffer.writeInt(type);
        sendBuffer.writeInt(session);
        sendBuffer.writeInt(length);
        sendBuffer.writeBytes(from, length);
        return sendBuffer;
    }
}
