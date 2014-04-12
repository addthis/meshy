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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;


public abstract class TargetHandler implements SessionHandler {

    protected static final Logger log = LoggerFactory.getLogger(TargetHandler.class);
    private final String className = getClass().getName();
    private final String shortName = className.substring(className.lastIndexOf(".") + 1);
    private final AtomicBoolean complete = new AtomicBoolean(false);
    private final AtomicBoolean waited = new AtomicBoolean(false);
    private final CountDownLatch latch = new CountDownLatch(1);

    private MeshyServer master;
    private ChannelState channelState;
    private int session;

    public TargetHandler() {
    }

    public void setContext(MeshyServer master, ChannelState state, int session) {
        this.master = master;
        this.channelState = state;
        this.session = session;
    }

    protected Objects.ToStringHelper toStringHelper() {
        return Objects.toStringHelper(this)
                .add("shortName", shortName)
                .add("channelState", channelState.getName())
                .add("session", session)
                .add("complete", complete)
                .add("waited", waited);
    }

    @Override
    public String toString() {
        return toStringHelper().toString();
    }

    public ChannelState getChannelState() {
        return channelState;
    }

    public MeshyServer getChannelMaster() {
        return master;
    }

    public int getSessionId() {
        return session;
    }

    public void send(ByteBuf from, int length) {
        log.trace("{} send.buf [{}] {}", this, length, from);
        channelState.send(ChannelState.allocateSendBuffer(MeshyConstants.KEY_RESPONSE, session, from, length), null, length);
    }

    public boolean send(ByteBuf data) {
        return send(data, null);
    }

    public void send(ByteBuf data, int off, int len, SendWatcher watcher) {
        log.trace("{} send {} o={} l={}", this, data.readableBytes(), off, len);
        channelState.send(ChannelState.allocateSendBuffer(MeshyConstants.KEY_RESPONSE, session, data, off, len), watcher, len);
    }

    public ByteBuf getSendBuffer(int length) {
        return ChannelState.allocateSendBuffer(MeshyConstants.KEY_RESPONSE, session, length);
    }

    public boolean send(ByteBuf buffer, SendWatcher watcher) {
        if (log.isTraceEnabled()) {
            log.trace("{} send b={} l={}", this, buffer, buffer.readableBytes());
        }
        int length = buffer.readableBytes();
        return channelState.send(buffer, watcher, length);
    }

    @Override
    public boolean sendComplete() {
        return send(Unpooled.EMPTY_BUFFER, null);
    }

    @Override
    public void receive(ChannelState state, int receivingSession, ByteBuf buffer) throws Exception {
        assert this.channelState == state;
        assert this.session == receivingSession;
        log.debug("{} receive [{}]", this, receivingSession);
        receive(buffer);
    }

    @Override
    public void receiveComplete(ChannelState state, int completedSession) throws Exception {
        assert this.channelState == state;
        assert this.session == completedSession;
        log.debug("{} receiveComplete.1 [{}]", this, completedSession);
        if (!state.getChannel().isOpen()) {
            channelClosed();
        }
        receiveComplete(completedSession);
    }

    @Override
    public void receiveComplete(int completedSession) throws Exception {
        assert this.session == completedSession;
        log.debug("{} receiveComplete.2 [{}]", this, completedSession);
        // ensure this is only called once
        if (complete.compareAndSet(false, true)) {
            receiveComplete();
            latch.countDown();
        }
    }

    @Override
    public void waitComplete() {
        // this is technically incorrect, but prevents lockups
        if (waited.compareAndSet(false, true)) {
            try {
                latch.await();
            } catch (Exception ex) {
                log.error("Swallowing exception while waitComplete() on targetHandler", ex);
            }
        }
    }

    public abstract void channelClosed();

    public abstract void receive(ByteBuf buffer) throws Exception;

    public abstract void receiveComplete() throws Exception;
}
