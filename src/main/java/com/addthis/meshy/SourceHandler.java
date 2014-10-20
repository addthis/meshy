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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Parameter;

import com.addthis.meshy.netty.DummyChannelGroup;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.ChannelGroupFutureListener;
import org.jboss.netty.channel.group.DefaultChannelGroupFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;


public abstract class SourceHandler implements SessionHandler {

    private static final Logger log = LoggerFactory.getLogger(SourceHandler.class);
    static final int DEFAULT_COMPLETE_TIMEOUT = Parameter.intValue("meshy.complete.timeout", 120);
    static final int DEFAULT_RESPONSE_TIMEOUT = Parameter.intValue("meshy.source.timeout", 0);
    static final boolean SLOW_SLOW_CHANNELS = Parameter.boolValue("meshy.source.closeSlow", false);
    static final boolean DISABLE_CREATION_FRAMES = Parameter.boolValue("meshy.source.noCreationFrames", false);

    // only used by response watcher
    static final Set<SourceHandler> activeSources = Collections.newSetFromMap(new ConcurrentHashMap<SourceHandler, Boolean>());
    // TODO: use scheduled thread pool
    static final Thread responseWatcher = new Thread("Source Response Watcher") {
        {
            setDaemon(true);
            start();
        }

        @Override
        public void run() {
            while (true) {
                try {
                    sleep(1000);
                } catch (Exception ignored) {
                    break;
                }
                probeActiveSources();
            }
            log.info(this + " exiting");
        }

        private void probeActiveSources() {
            for (SourceHandler handler : activeSources) {
                handler.handleChannelTimeouts();
            }
        }
    };

    private final Class<? extends TargetHandler> targetClass;
    private final String className = getClass().getName();
    private final String shortName = className.substring(className.lastIndexOf(".") + 1);
    private final AtomicBoolean sent = new AtomicBoolean(false);
    private final AtomicBoolean complete = new AtomicBoolean(false);
    private final AtomicBoolean waited = new AtomicBoolean(false);
    private final Semaphore gate = new Semaphore(0);
    private final ChannelMaster master;

    private int session;
    private int targetHandler;
    private long readTime;
    private long readTimeout;
    private long completeTimeout;
    protected Set<Channel> channels;

    /**
     * This constructor does not perform initialization during construction and therefore does not escape "this".
     * The boolean parameter is expected to always be true and is only included to differentiate the signature from
     * the otherwise identical constructor that does escape "this".
     */
    protected SourceHandler(ChannelMaster master,
                            Class<? extends TargetHandler> targetClass,
                            /* should always be true */ boolean skipInit) {
        checkArgument(skipInit, "skipInit must always be true");
        this.master = master;
        this.targetClass = targetClass;
    }

    public SourceHandler(ChannelMaster master, Class<? extends TargetHandler> targetClass) {
        this(master, targetClass, MeshyConstants.LINK_ALL);
    }

    public SourceHandler(ChannelMaster master, Class<? extends TargetHandler> targetClass, String targetUuid) {
        this(master, targetClass, true);
        start(targetUuid);
    }

    protected final void start() {
        start(MeshyConstants.LINK_ALL);
    }

    protected void start(String targetUuid) {
        this.readTime = JitterClock.globalTime();
        Collection<ChannelState> matches = master.getChannels(targetUuid);
        if (matches.isEmpty()) {
            throw new ChannelException("no matching mesh peers");
        }
        this.session = master.newSession();
        Set<Channel> group = new HashSet<>(matches.size());
        for (ChannelState state : matches) {
            group.add(state.getChannel());
        }
        this.channels = Collections.synchronizedSet(group);
        this.targetHandler = master.targetHandlerId(targetClass);
        setReadTimeout(DEFAULT_RESPONSE_TIMEOUT);
        setCompleteTimeout(DEFAULT_COMPLETE_TIMEOUT);
        activeSources.add(this);
        for (ChannelState state : matches) {
            /* add channel callback path to source */
            state.addSourceHandler(session, this);
            if (!state.getChannel().isOpen()) {
                channels.remove(state.getChannel()); // may or may not be needed
            }
        }
        log.debug("{} start target={} uuid={} group={} sessionID={}",
                  this, targetHandler, targetUuid, channels, session);
    }

    @Override
    public String toString() {
        return master + "[Source:" + shortName + ":s=" + session + ",h=" + targetHandler + ",c=" + (channels != null ? channels.size() : "null") + "]";
    }

    private void handleChannelTimeouts() {
        if ((readTimeout > 0) && ((JitterClock.globalTime() - readTime) > readTimeout)) {
            log.info("{} response timeout on channel: {}", this, channelsToList());
            if (SLOW_SLOW_CHANNELS) {
                log.warn("closing {} channel(s)", channels.size());
                synchronized (channels) {
                    for (Channel channel : channels) {
                        channel.close();
                    }
                }
            }
            channels.clear();
            try {
                receiveComplete(session);
            } catch (Exception ex) {
                log.error("Swallowing exception while handling channel timeout", ex);
            }
        }
    }

    public ChannelMaster getChannelMaster() {
        return master;
    }

    public void setReadTimeout(int seconds) {
        readTimeout = (long) (seconds * 1000);
    }

    public void setCompleteTimeout(int seconds) {
        completeTimeout = (long) (seconds * 1000);
    }

    public int getPeerCount() {
        return channels.size();
    }

    public String getPeerString() {
        StringBuilder sb = new StringBuilder(10 * channels.size());
        synchronized (channels) {
            for (Channel channel : channels) {
                if (sb.length() > 0) {
                    sb.append(",");
                }
                sb.append(channel.getRemoteAddress());
            }
        }
        return sb.toString();
    }

    @Override
    public boolean sendComplete() {
        return send(MeshyConstants.EMPTY_BYTES, null);
    }

    public boolean send(byte[] data) {
        return send(data, null);
    }

    @Override
    public final boolean send(byte[] data, final SendWatcher watcher) {
        synchronized (channels) {
            if (channels.isEmpty()) {
                return false;
            }

            int sendType = MeshyConstants.KEY_EXISTING;
            if (sent.compareAndSet(false, true) || DISABLE_CREATION_FRAMES) {
                sendType = targetHandler;
            }

            final ChannelBuffer buffer = ChannelState.allocateSendBuffer(sendType, session, data);
            final int reportBytes = data.length;
            final int peerCount = channels.size();

            log.trace("{} send {} to {}", this, buffer.capacity(), peerCount);
            List<ChannelFuture> futures = new ArrayList<>(peerCount);
            for (Channel c : channels) {
                futures.add(c.write(buffer.duplicate()));
            }
            ChannelGroupFuture future = new DefaultChannelGroupFuture(DummyChannelGroup.DUMMY, futures);
            future.addListener(new ChannelGroupFutureListener() {
                @Override
                public void operationComplete(ChannelGroupFuture future) throws Exception {
                    master.sentBytes(reportBytes * peerCount);
                    ChannelState.returnSendBuffer(buffer);
                    if (watcher != null) {
                        watcher.sendFinished(reportBytes);
                    }
                }
            });
            return true;
        }
    }

    @Override
    public void receive(ChannelState state, int receivingSession, int length, ChannelBuffer buffer) throws Exception {
        this.readTime = JitterClock.globalTime();
        log.debug("{} receive [{}] l={}", this, receivingSession, length);
        receive(state, length, buffer);
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

    private void receiveComplete(int completedSession) throws Exception {
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
                stringBuilder.append(channel.getRemoteAddress().toString());
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

    public abstract void receive(ChannelState state, int length, ChannelBuffer buffer) throws Exception;

    public abstract void receiveComplete() throws Exception;
}
