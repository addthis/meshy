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
package com.addthis.meshy.service.file;

import java.io.IOException;

import java.net.InetSocketAddress;

import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Parameter;

import com.addthis.meshy.ChannelMaster;
import com.addthis.meshy.ChannelState;
import com.addthis.meshy.Meshy;
import com.addthis.meshy.TargetHandler;
import com.addthis.meshy.VirtualFileFilter;
import com.addthis.meshy.VirtualFileReference;
import com.addthis.meshy.VirtualFileSystem;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelException;

public class FileTarget extends TargetHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(FileTarget.class);

    static final int finderWarnTime = Parameter.intValue("meshy.finder.warnTime", 10000);
    static final int debugCacheLine = Parameter.intValue("meshy.finder.debug.cacheLine", 5000);

    //  Metrics version of 'finds' is handled by the localFindTimer's meter
    static final AtomicInteger finds = new AtomicInteger(0);
    static final Meter fileFindMeter = Metrics.newMeter(FileTarget.class, "allFinds", "found", TimeUnit.SECONDS);
    static final AtomicInteger found = new AtomicInteger(0);
    static final Counter findsRunning = Metrics.newCounter(FileTarget.class, "allFinds", "running");
    static final AtomicLong findTime = new AtomicLong(0);
    static final Timer localFindTimer = Metrics.newTimer(FileTarget.class, "localFinds", "timer");
    static final AtomicLong findTimeLocal = new AtomicLong(0);

    static final int finderThreads = Parameter.intValue("meshy.finder.threads", 2);
    static final int finderQueueSafetyDrop = Parameter.intValue("meshy.finder.safety.drop", Integer.MAX_VALUE);
    static final Gauge<Integer> finderQueueSize = Metrics.newGauge(FileTarget.class, "allFinds", "queued", new Gauge<Integer>() {
        @Override
        public Integer value() {
            return finderQueue.size();
        }
    });

    static final LinkedBlockingQueue<Runnable> finderQueue = new LinkedBlockingQueue<>(finderQueueSafetyDrop);

    private static final ExecutorService finderPool = MoreExecutors
            .getExitingExecutorService(new ThreadPoolExecutor(finderThreads, finderThreads, 0L, TimeUnit.MILLISECONDS,
                    finderQueue,
                    new ThreadFactoryBuilder().setNameFormat("finder-%d").build()), 1, TimeUnit.SECONDS);

    private final long markTime = System.currentTimeMillis();

    private final AtomicBoolean firstDone = new AtomicBoolean(false);
    private final AtomicBoolean canceled = new AtomicBoolean(false);
    private final AtomicInteger currentWindow = new AtomicInteger(0);
    private final LinkedList<String> paths = new LinkedList<>();

    private boolean pathsComplete = false;
    private boolean forwardMetaData = false;
    private Future<?> findTask = null;
    private String scope = null;

    private volatile ForwardingFileSource remoteSource = null;

    @Override
    public void receive(int length, ByteBuf buffer) throws Exception {
        if (!pathsComplete) {
            byte[] bytes = Meshy.getBytes(length, buffer);
            if (scope == null) {
                scope = Bytes.toString(bytes);
                log.trace("{} recv scope={}", this, scope);
            } else if ((length == 1) && (bytes[0] == -1)) {
                // a byte array of length one containing only "-1" signals the end of the paths listing.
                // this is an invalid utf8 string, and we use it because in this protocol, we cannot send an
                // empty string.
                pathsComplete = true;
            } else {
                paths.add(Bytes.toString(bytes));
            }
        } else {
            int additionalWindow = Bytes.readInt(Meshy.getInput(length, buffer));
            ForwardingFileSource remoteSourceRef = remoteSource;
            if (remoteSourceRef != null) {
                log.debug("received additional window allotment ({}) for {}", additionalWindow, this);
                remoteSourceRef.increaseWindow(additionalWindow);
            } else {
                currentWindow.getAndAdd(additionalWindow);
            }
            if (findTask == null) {
                startFindTask();
            }
        }
    }

    private void startFindTask() {
        try {
            if (!canceled.get()) {
                findTask = finderPool.submit(this);
            } else {
                findTask = Futures.immediateCancelledFuture();
                log.debug("skipping execution of canceled file find");
            }
        } catch (RejectedExecutionException ignored) {
            log.warn("dropping find @ queue={} paths={}", finderQueue.size(), paths);
            cancelFindTask();
        }
    }

    @Override
    public void receiveComplete() throws IOException {
        if (findTask == null) {
            // legacy client support
            currentWindow.set(Integer.MAX_VALUE);
            startFindTask();
        } else {
            log.debug("canceling find task for {} on receive complete", this);
            cancelFindTask();
        }
    }

    @Override
    public void channelClosed() {
        log.debug("canceling find task for {} on channel close", this);
        cancelFindTask();
    }

    private void cancelFindTask() {
        canceled.set(true);
        if (findTask != null) {
            findTask.cancel(false);
        }
        if (remoteSource != null) {
            log.debug("sending complete to remote source: {}", remoteSource);
            remoteSource.sendComplete();
        } else {
            sendComplete();
        }
    }

    @Override
    public boolean sendComplete() {
        log.debug("sending complete for {}, but first triggering auto receive complete", this);
        autoReceiveComplete();
        return super.sendComplete();
    }

    @Override
    public void run() {
        try {
            doFind();
        } catch (Exception e) {
            log.error("FileTarget:run() error", e);
        }
    }

    /**
     * perform the find. called by finder threads from an executor service. see run()
     */
    public void doFind() throws IOException {
        findsRunning.inc();
        try {
            //should we ask other meshy nodes for file references as well?
            final boolean remote = scope.startsWith("local");
            log.debug("{} starting-find={}", this, scope);
            if (remote) { //yes, ask other meshy nodes (and ourselves)
                forwardMetaData = "localF".equals(scope);
                try {
                    ForwardingFileSource newRemoteSource = new ForwardingFileSource(getChannelMaster());
                    newRemoteSource.requestLocalFiles(paths.toArray(new String[paths.size()]));
                    remoteSource = newRemoteSource;
                    if (canceled.get()) {
                        remoteSource.sendComplete();
                    } else {
                        // catch any weird, late window changes
                        int extraWindow = currentWindow.getAndSet(0);
                        if (extraWindow > 0) {
                            remoteSource.increaseWindow(extraWindow);
                        }
                    }
                } catch (ChannelException ignored) {
                    // can happen when there are no remote hosts
                    if (forwardMetaData) {
                        forwardPeerList(Collections.<Channel>emptyList());
                    }
                }
            } //else -- just look ourselves

            //Local filesystem find. Done in both cases.
            WalkState walkState = new WalkState();
            long localStart = System.currentTimeMillis();
            for (String onepath : paths) {
                for (VirtualFileSystem vfs : getChannelMaster().getFileSystems()) {
                    VFSPath path = new VFSPath(vfs.tokenizePath(onepath));
                    log.trace("{} recv.walk vfs={} path={}", this, vfs, path);
                    walkSafe(walkState, Long.toString(vfs.hashCode()), vfs.getFileRoot(), path);
                }
            }
            long localRunTime = System.currentTimeMillis() - localStart;
            if (localRunTime > finderWarnTime) {
                log.warn("{} slow find ({}) for {}", this, localRunTime, paths);
            }
            finds.incrementAndGet();
            findTimeLocal.addAndGet(localRunTime);
            localFindTimer.update(localRunTime, TimeUnit.MILLISECONDS);
        } finally {
            if (forwardMetaData) {
                FileReference flagRef = new FileReference("localfind", 0, 0);
                FileTarget.this.send(flagRef.encode(null));
            }
            //Expected conditions under which we should cleanup: If we do not expect a response from the mesh
            // (fileSource == null implies no remote request or an error attempting it) or if the mesh has
            // already finished responding.
            if ((remoteSource == null) || !firstDone.compareAndSet(false, true)) {
                log.debug("sending complete from local find thread");
                findTime.addAndGet(System.currentTimeMillis() - markTime);
                findsRunning.dec();
                getChannelState().getChannel().eventLoop().execute(FileTarget.this::sendComplete);
            } else if (remoteSource != null) {
                remoteSource.increaseWindow(currentWindow.getAndSet(-1));
            }
        }
    }

    /**
     * Wrapper around walk with a try/catch that swallows all exceptions (and prints some statements). Presumably
     * this is to help make finder threads unkillable since they are started only once.
     */
    private void walkSafe(final WalkState state, final String vfsKey, final VirtualFileReference ref, final VFSPath path) {
        try {
            walk(state, vfsKey, ref, path);
        } catch (Exception ex) {
            log.warn("walk fail {} @ {}", ref, path.getRealPath(), ex);
        }
    }

    /**
     * Recursively (though it calls walkSafe for its recursion) walk through the filesystem to locate the
     * requested files. Returns void because it streams the results out through the mesh network in place
     * instead of appending to a results list. Remember that send() is asynchronous so this does not block
     * on network activity; it may block on disk i/o or various local handler implementations. Also the
     * results that it sends out may not be recieved as fast as imagined (queuing in meshy output).
     */
    private void walk(final WalkState state, final String vfsKey, final VirtualFileReference ref, final VFSPath path) throws Exception {
        if (canceled.get()) {
            return;
        }
        String token = path.getToken();
        log.trace("walk token={} ref={} path={}", token, ref, path);
        final boolean all = "*".equals(token);
        final boolean startsWith = !all && token.endsWith("*");
        if (startsWith) {
            token = token.substring(0, token.length() - 1);
        }
        final boolean endsWith = !all && token.startsWith("*");
        if (endsWith) {
            token = token.substring(1);
        }
        final boolean asDir = path.hasMoreTokens();
        final VirtualFileFilter filter = new Filter(token, all, startsWith, endsWith);
        /* possible now b/c of change to follow sym links */
        if (asDir) {
            Iterator<VirtualFileReference> files = ref.listFiles(filter);
            log.trace("asDir=true filter={} files={}", filter, files);
            if (files == null) {
                return;
            }
            while (files.hasNext() && !canceled.get()) {
                VirtualFileReference next = files.next();
                if (path.push(next.getName())) {
                    walkSafe(state, vfsKey, next, path);
                    state.dirs++;
                    path.pop();
                }
            }
        } else {
            long mark = debugCacheLine > 0 ? System.currentTimeMillis() : 0;
            Iterator<VirtualFileReference> files = ref.listFiles(filter);
            log.trace("asDir=false filter={} files={}", filter, files);
            if (files == null) {
                return;
            }
            while (files.hasNext() && !canceled.get()) {
                VirtualFileReference next = files.next();
                FileReference fileRef = new FileReference(path.getRealPath(), next);
                sendLocalFileRef(fileRef);
            }
            state.files += found.get();
            if (debugCacheLine > 0) {
                long time = System.currentTimeMillis() - mark;
                if (time > debugCacheLine) {
                    String pathString = vfsKey + ":" + path.getRealPath() + "[" + token + "]";
                    log.warn("slow ({}) for {} {{}}", time, pathString, state);
                }
            }
        }
    }

    private boolean sendLocalFileRef(FileReference fileReference) {
        while((currentWindow.get() == 0) && !canceled.get()) {
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        }
        boolean wasSent = send(fileReference.encode(getChannelMaster().getUUID()));
        if (wasSent) {
            found.incrementAndGet();
            fileFindMeter.mark();
            currentWindow.decrementAndGet();
        }
        return wasSent;
    }

    private void forwardPeerList(Collection<Channel> peerList) {
        int peerCount = peerList.size();
        FileReference flagRef = new FileReference("peers", 0, peerCount);
        send(flagRef.encode(FileTarget.peersToString(peerList)));
    }

    private static String peersToString(Iterable<Channel> peers) {
        try {
            StringBuilder sb = new StringBuilder();
            for (Channel peer : peers) {
                if (sb.length() > 0) {
                    sb.append(',');
                }
                sb.append(((InetSocketAddress) peer.remoteAddress()).getHostName());
            }
            return sb.toString();
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    private static final class VFSPath {

        final Deque<String> path = new LinkedList<>();
        final String[] tokens;
        String token;
        int pos;

        VFSPath(String... tokens) {
            this.tokens = tokens;
            push("");
        }

        @Override
        public String toString() {
            return "VFSPath:" + path + '@' + pos + '=' + token;
        }

        String getToken() {
            return token;
        }

        String getRealPath() {
            StringBuilder sb = new StringBuilder();
            for (String p : path) {
                if (!p.isEmpty()) {
                    sb.append('/');
                    sb.append(p);
                }
            }
            return sb.toString();
        }

        boolean hasMoreTokens() {
            return pos < tokens.length;
        }

        boolean push(String element) {
            if (pos < tokens.length) {
                token = tokens[pos++];
                path.addLast(element);
                return true;
            }
            return false;
        }

        boolean pop() {
            if (pos > 0) {
                token = tokens[--pos];
                path.removeLast();
                return true;
            }
            return false;
        }
    }

    private static final class WalkState {

        int dirs;
        int files;

        @Override
        public String toString() {
            return "dirs=" + dirs + ";files=" + files;
        }
    }

    private class ForwardingFileSource extends FileSource {

        private final AtomicBoolean doComplete = new AtomicBoolean();
        private final ConcurrentHashMultiset<Channel> windows = ConcurrentHashMultiset.create();

        public ForwardingFileSource(ChannelMaster master) {
            super(master);
        }

        @Override protected void sendInitialWindowing() {
            long totalInitialWindow = FileTarget.this.currentWindow.getAndSet(0);
            if (totalInitialWindow > 0) {
                synchronized (channels) {
                    long peerCount = getPeerCount() + 1;
                    // add peerCount - 1 to round up the division. We'd rather have too many than too few
                    int windowPerPeer = (int) ((totalInitialWindow + peerCount - 1) / peerCount);
                    for (Channel channel : channels) {
                        windows.add(channel, windowPerPeer);
                    }
                    send(Bytes.toBytes(windowPerPeer));
                    FileTarget.this.currentWindow.addAndGet(windowPerPeer);
                }
            } else {
                FileSource.log.warn("initial window was not a positive number: {}", totalInitialWindow);
            }
        }

        void increaseWindow(int additionalWindow) {
            if (additionalWindow < 0) {
                FileSource.log.warn("Someone requested a negative amount of window allotment: {}", additionalWindow);
                return;
            } else if (additionalWindow == 0) {
                FileSource.log.debug("Someone requested exactly 0 more window allotment");
                return;
            }
            synchronized (channels) {
                int peerCount = getPeerCount();
                int totalNewWindow = windows.size() + additionalWindow;
                int totalAddedWindow = 0;

                int localWindow = FileTarget.this.currentWindow.get();
                if (localWindow != -1) {
                    int peerCountWithLocal = peerCount + 1;
                    int totalNewWindowWithLocal = totalNewWindow + localWindow;
                    int windowPerPeerWithLocal = (totalNewWindowWithLocal + peerCountWithLocal - 1) / peerCountWithLocal;
                    int localWindowTargetDelta = windowPerPeerWithLocal - localWindow;
                    if (localWindowTargetDelta > 0) {
                        int prevLocalWindow = FileTarget.this.currentWindow.getAndAdd(localWindowTargetDelta);
                        if (prevLocalWindow != -1) {
                            peerCount = peerCountWithLocal;
                            totalNewWindow = totalNewWindowWithLocal;
                            totalAddedWindow = localWindowTargetDelta;
                        } else {
                            FileTarget.this.currentWindow.set(-1);
                        }
                    }
                }
                if (peerCount <= 0) {
                    return;
                }
                int windowPerPeer = (totalNewWindow + peerCount - 1) / peerCount;
                for (Channel channel : channels) {
                    if (totalAddedWindow >= additionalWindow) {
                        break;
                    }
                    int prevCount = windows.count(channel);
                    if (prevCount >= windowPerPeer) {
                        continue;
                    }
                    int countChange = Math.min(windowPerPeer - prevCount, additionalWindow - totalAddedWindow);
                    windows.add(channel, countChange);
                    totalAddedWindow += countChange;
                    sendToSingleTarget(channel, Bytes.toBytes(countChange));
                }
                if (totalAddedWindow < additionalWindow) {
                    FileSource.log.warn("Failed to allocate all of the requested window allotment ({} < {})",
                                        totalAddedWindow, additionalWindow);
                }
            }
        }

        @Override
        public void receive(ChannelState state, int length, ByteBuf buffer) throws Exception {
            windows.remove(state.getChannel());
            FileTarget.this.send(Meshy.getBytes(length, buffer));
        }

        @Override
        protected void start(String targetUuid) {
            super.start(targetUuid);
            if (forwardMetaData) {
                synchronized (channels) {
                    forwardPeerList(channels);
                }
            }
        }

        // called per individual remote mesh node response complete
        @Override
        public void receiveComplete(ChannelState state, int completedSession) throws Exception {
            super.receiveComplete(state, completedSession);
            if (forwardMetaData) {
                int peerCount = getPeerCount();
                FileReference flagRef = new FileReference("response", 0, peerCount);
                FileTarget.this.send(flagRef.encode(state.getChannelRemoteAddress().getHostName()));
            }
            if (doComplete.compareAndSet(true, false) && !firstDone.compareAndSet(false, true)) {
                findTime.addAndGet(System.currentTimeMillis() - markTime);
                findsRunning.dec();
                FileSource.log.debug("sending complete from remote source ({}) thread", this);
                // No. Shortcuts.
                getChannelState().getChannel().eventLoop().execute(FileTarget.this::sendComplete);
            } else {
                int remainingWindow = windows.setCount(state.getChannel(), 0);
                increaseWindow(remainingWindow);
            }
        }

        @Override
        public void receiveComplete() throws Exception {
            FileSource.log.debug("receive complete on remote source ({}) for proxy {}", this, FileTarget.this);
            doComplete.set(true);
        }
    }
}
