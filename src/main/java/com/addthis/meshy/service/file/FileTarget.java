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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
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
import com.addthis.basis.util.Strings;

import com.addthis.meshy.ChannelMaster;
import com.addthis.meshy.ChannelState;
import com.addthis.meshy.Meshy;
import com.addthis.meshy.MeshyConstants;
import com.addthis.meshy.MeshyServer;
import com.addthis.meshy.TargetHandler;
import com.addthis.meshy.VirtualFileFilter;
import com.addthis.meshy.VirtualFileReference;
import com.addthis.meshy.VirtualFileSystem;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTarget extends TargetHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(FileTarget.class);

    static final int dirCacheAge = Parameter.intValue("meshy.file.dirCacheAge", 60000);
    static final int dirCacheSize = Parameter.intValue("meshy.file.dirCacheSize", 50);
    static final int maxCacheTokens = Parameter.intValue("meshy.file.dirCacheTokens", 500);
    static final int finderWarnTime = Parameter.intValue("meshy.finder.warnTime", 10000);
    static final int debugCacheLine = Parameter.intValue("meshy.finder.debug.cacheLine", 5000);

    static final Meter cacheHitsMeter = Metrics.newMeter(FileTarget.class, "dirCacheHits", "dirCacheHits", TimeUnit.SECONDS);
    static final AtomicInteger cacheHits = new AtomicInteger(0);
    static final Meter cacheEvictsMeter = Metrics.newMeter(FileTarget.class, "dirCacheEvicts", "dirCacheEvicts", TimeUnit.SECONDS);
    static final AtomicInteger cacheEvicts = new AtomicInteger(0);
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

    protected static final HashMap<ChannelMaster, VFSDirCache> cacheByMaster = new HashMap<>(1);

    private final long markTime = System.currentTimeMillis();

    private final AtomicBoolean firstDone = new AtomicBoolean(false);
    private final LinkedList<String> paths = new LinkedList<>();
    private boolean canceled = false;
    private boolean forwardMetaData = false;
    private Future<?> findTask = null;
    private VFSDirCache cache;
    private String scope = null;

    @Override
    public void setContext(MeshyServer master, ChannelState state, int session) {
        super.setContext(master, state, session);
        synchronized (cacheByMaster) {
            cache = cacheByMaster.get(master);
            if (cache == null) {
                cache = new VFSDirCache();
                cacheByMaster.put(master, cache);
            }
        }
    }

    @Override
    public void receive(int length, ChannelBuffer buffer) throws Exception {
        final String msg = Bytes.toString(Meshy.getBytes(length, buffer));
        log.trace("{} recv scope={} msg={}", this, scope, msg);
        if (scope == null) {
            scope = msg;
        } else {
            paths.add(msg);
        }
    }

    @Override
    public void receiveComplete() throws IOException {
        try {
            if (!canceled) {
                findTask = finderPool.submit(this);
            } else {
                log.debug("skipping execution of canceled file find");
            }
        } catch (RejectedExecutionException ignored) {
            log.warn("dropping find @ queue={} paths={}", finderQueue.size(), paths);
            dropFind();
        } catch (Exception ex) {
            log.warn("FileTarget:receiveComplete() eror", ex);
        }
    }

    @Override
    public void channelClosed() {
        // TODO : more robust cancelation support. eg. cancel remotes as in stream service
        if (findTask != null) {
            findTask.cancel(false); // interrupting in some of those places would be bad
        }
        canceled = true;
    }

    private void dropFind() {
        sendComplete();
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
        FileSource fileSource = null;
        findsRunning.inc();
        try {
            //should we ask other meshy nodes for file references as well?
            final boolean remote = scope.startsWith("local");
            log.debug("{} starting-find={}", this, scope);
            if (remote) { //yes, ask other meshy nodes (and ourselves)
                forwardMetaData = "localF".equals(scope);
                try {
                    fileSource = new ForwardingFileSource(getChannelMaster(), MeshyConstants.LINK_NAMED,
                            paths.toArray(new String[paths.size()]));
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
            //Expected conditions under which we should cleanup. If we do not expect a response from the mesh
            // (fileSource == null implies no remote request or an error attempting it; peerCount == 0 implies
            // something similar) or if the mesh has already finished responding.
            if (fileSource == null || fileSource.getPeerCount() == 0 || !firstDone.compareAndSet(false, true)) {
                findTime.addAndGet(System.currentTimeMillis() - markTime);
                findsRunning.dec();
                sendComplete();
            }
        }
    }

    /**
     * like interning, but less OOM likely (one hopes)
     */
    private final LinkedHashMap<String, String> pathStrings = new LinkedHashMap<String, String>() {
        @Override
        public boolean removeEldestEntry(Map.Entry<String, String> entry) {
            return size() > maxCacheTokens;
        }
    };

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
        String token = path.getToken();
        if (log.isTraceEnabled()) {
            log.trace("walk token=" + token + " ref=" + ref + " path=" + path);
        }
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
        final String hostUuid = getChannelMaster().getUUID();
        /* possible now b/c of change to follow sym links */
        if (asDir) {
            Iterator<VirtualFileReference> files = ref.listFiles(filter);
            if (log.isTraceEnabled()) {
                log.trace("asDir=true filter=" + filter + " hostUuid=" + hostUuid + " files=" + files);
            }
            if (files == null) {
                return;
            }
            while (files.hasNext()) {
                VirtualFileReference next = files.next();
                if (path.push(next.getName())) {
                    walkSafe(state, vfsKey, next, path);
                    state.dirs++;
                    path.pop();
                }
            }
        } else {
            long mark = debugCacheLine > 0 ? System.currentTimeMillis() : 0;
            String pathString = null;
            if (dirCacheSize > 0) { // if skipping questionable dir cache, then skip unused concurrency
                pathString = Strings.cat(vfsKey, ":", path.getRealPath(), "[", token, "]");
            /* this allows synchronizing on the path String */
                synchronized (pathStrings) {
                    String interned = pathStrings.get(pathString);
                    if (interned == null) {
                        pathStrings.put(pathString, pathString);
                        interned = pathString;
                    }
                    pathString = interned;
                }
                VFSDirCacheLine cacheLine = null;
            /* create "directory" cache line if missing */
                synchronized (cache) {
                    cacheLine = cache.get(pathString);
                    if (cacheLine == null || !cacheLine.isValid()) {
                        if (log.isTraceEnabled()) {
                            log.trace("new cache-line for " + pathString + " was " + cacheLine);
                        }
                        cacheLine = new VFSDirCacheLine(ref);
                        cache.put(pathString, cacheLine);
                    } else {
                        if (log.isTraceEnabled()) {
                            log.trace("old cache-line for " + pathString + " = " + cacheLine);
                        }
                        cacheHits.incrementAndGet();
                        cacheHitsMeter.mark();
                    }
                }
            /* prevent same query from multiple clients simultaneously */
                synchronized (cacheLine) {
                /* use cache-line if not empty */
                    if (!cacheLine.lines.isEmpty()) {
                        for (FileReference cacheRef : cacheLine.lines) {
                            if (log.isTraceEnabled()) {
                                log.trace("cache.send " + ref + " from " + cacheRef + " key=" + pathString);
                            }
                            send(cacheRef.encode(hostUuid));
                            found.incrementAndGet();
                            fileFindMeter.mark();
                        }
                        return;
                    }
                /* otherwise populate cache line */
                    Iterator<VirtualFileReference> files = ref.listFiles(filter);
                    if (log.isTraceEnabled()) {
                        log.trace("asDir=false filter=" + filter + " hostUuid=" + hostUuid + " files=" + files);
                    }
                    if (files == null) {
                        return;
                    }
                    while (files.hasNext()) {
                        VirtualFileReference next = files.next();
                        FileReference cacheRef = new FileReference(path.getRealPath(), next);
                        cacheLine.lines.add(cacheRef);
                        if (log.isTraceEnabled()) {
                            log.trace("local.send " + cacheRef + " cache to " + pathString + " in " + cacheLine.hashCode());
                        }
                        send(cacheRef.encode(hostUuid));
                        found.incrementAndGet();
                        fileFindMeter.mark();
                    }
                }
            } else {
                Iterator<VirtualFileReference> files = ref.listFiles(filter);
                if (log.isTraceEnabled()) {
                    log.trace("asDir=false filter=" + filter + " hostUuid=" + hostUuid + " files=" + files);
                }
                if (files == null) {
                    return;
                }
                while (files.hasNext()) {
                    VirtualFileReference next = files.next();
                    FileReference cacheRef = new FileReference(path.getRealPath(), next);
                    send(cacheRef.encode(hostUuid));
                    found.incrementAndGet();
                    fileFindMeter.mark();
                }
            }
            state.files += found.get();
            if (debugCacheLine > 0) {
                long time = System.currentTimeMillis() - mark;
                if (time > debugCacheLine) {
                    if (pathString == null) {
                        pathString = Strings.cat(vfsKey, ":", path.getRealPath(), "[", token, "]");
                    }
                    log.warn("slow cache fill (" + time + ") for " + pathString + " {" + state + '}');
                }
            }
        }
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
                sb.append(((InetSocketAddress) peer.getRemoteAddress()).getHostName());
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

        public ForwardingFileSource(ChannelMaster master, String nameFilter, String[] files) {
            super(master, nameFilter, files);
        }

        @Override
        public void receive(ChannelState state, int length, ChannelBuffer buffer) throws Exception {
            FileTarget.this.send(Meshy.getBytes(length, buffer));
        }

        @Override
        public void init(int session, int targetHandler, Set<Channel> group) {
            if (forwardMetaData) {
                //directly get size from group since channels is not set yet;
                forwardPeerList(group);
            }
            super.init(session, targetHandler, group);
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
            if (doComplete.compareAndSet(true, false)) {
                if (!firstDone.compareAndSet(false, true)) {
                    findTime.addAndGet(System.currentTimeMillis() - markTime);
                    findsRunning.dec();
                    FileTarget.this.sendComplete();
                }
            }
        }

        @Override
        public void receiveComplete() throws Exception {
            doComplete.set(true);
        }
    }
}
