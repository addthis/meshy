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
package com.addthis.meshy.netty;

import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.JitterClock;

import com.addthis.meshy.ChannelState;
import com.addthis.meshy.SessionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.ChannelGroupFutureListener;


/**
 * Manages a single target.
 */
public abstract class SimpleSourceHandler extends ChannelDuplexHandler {

    private static final Logger log = LoggerFactory.getLogger(SimpleSourceHandler.class);

    private ChannelHandlerContext ctx;

    private long readTime;
    private long readTimeout;
    private long completeTimeout;
    private boolean closed;

    private final int session;
    private final int targetHandlerType;
    private final Channel targetChannel;

    public SimpleSourceHandler(Channel targetChannel, int session, int targetHandlerType,
            long readTimeout, long completeTimeout) {
        this.targetChannel = targetChannel;
        this.session = session;
        this.targetHandlerType = targetHandlerType;
        this.readTimeout = readTimeout;
        this.completeTimeout = completeTimeout;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
    }

    public int getPeerCount() {
        return closed ? 0 : 1;
    }

    public String getPeerString() {
        return targetChannel.remoteAddress().toString();
    }

    @Override
    public ChannelFuture sendComplete() {
        return send(Unpooled.EMPTY_BUFFER);
    }

    public ChannelFuture send(ByteBuf data) {
        return ctx.write(ChannelState.allocateSendBuffer(targetHandlerType, session, data));
    }

    /**
     * Called once a write operation is made. The write operation will write the messages through the
     * {@link io.netty.channel.ChannelPipeline}. Those are then ready to be flushed to the actual {@link Channel} once
     * {@link Channel#flush()} is called
     *
     * @param ctx               the {@link ChannelHandlerContext} for which the write operation is made
     * @param msg               the message to write
     * @param promise           the {@link ChannelPromise} to notify once the operation completes
     * @throws Exception        thrown if an error accour
     */
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

    }

    private boolean send(final ByteBuf buffer) {
        if (log.isTraceEnabled()) {
            log.trace(this + " send " + buffer.capacity() + " to " + channels.size());
        }
        if (!channels.isEmpty()) {
            final int peerCount = channels.size();
            if (sent.compareAndSet(false, true)) {
                try {
                    gate.acquire();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            ChannelGroupFuture future = channels.writeAndFlush(buffer);
            future.addListener(new ChannelGroupFutureListener() {
                @Override
                public void operationComplete(ChannelGroupFuture future) throws Exception {
                    master.sentBytes(reportBytes * peerCount);
                    buffer.release();
                    if (watcher != null) {
                        watcher.sendFinished(reportBytes);
                    }
                }
            });
            return true;
        }
        return false;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        this.readTime = JitterClock.globalTime();
        log.debug("{} receive [{}]", this, msg);
    }

    @Override
    public void receiveComplete(ChannelState state, int completedSession) throws Exception {
        log.debug("{} receiveComplete.1 [{}]", this, completedSession);
        Channel channel = state.getChannel();
        if (channel != null) {
            channels.remove(channel);
            if (!channel.isOpen()) {
                channelClosed(state);
            }
        }
        if (channels.isEmpty()) {
            receiveComplete(completedSession);
        }
    }

    @Override
    public void receiveComplete(int completedSession) throws Exception {
        log.debug("{} receiveComplete.2 [{}]", this, completedSession);
        // ensure this is only called once
        if (complete.compareAndSet(false, true)) {
            if (sent.get()) {
                gate.release();
            }
            receiveComplete();
            activeSources.remove(this);
        }
    }

    private String channelsToList() {
        StringBuilder stringBuilder = new StringBuilder(10 * channels.size());
        synchronized (channels) {
            for (Channel channel : channels) {
                stringBuilder.append(channel.remoteAddress().toString());
            }
        }
        return stringBuilder.toString();
    }

    @Override
    public void waitComplete() {
        // this is technically incorrect, but prevents lockups
        if (waited.compareAndSet(false, true) && sent.get()) {
            try {
                if (!gate.tryAcquire(completeTimeout, TimeUnit.MILLISECONDS)) {
                    log.warn("{} failed to waitComplete() normally from channels: {}", this, channelsToList());
                    activeSources.remove(this);
                }
            } catch (Exception ex) {
                log.error("Swallowing mystery exception", ex);
            }
        }
    }

    public abstract void channelClosed(ChannelState state);

    public abstract void receive(ChannelState state, ByteBuf in) throws Exception;

    public abstract void receiveComplete() throws Exception;
}
