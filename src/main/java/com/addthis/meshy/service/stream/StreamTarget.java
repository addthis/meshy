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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Parameter;

import com.addthis.meshy.Meshy;
import com.addthis.meshy.SendWatcher;
import com.addthis.meshy.TargetHandler;
import com.addthis.meshy.VirtualFileInput;
import com.addthis.meshy.VirtualFileReference;
import com.addthis.meshy.VirtualFileSystem;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

public class StreamTarget extends TargetHandler implements Runnable, SendWatcher {

    protected static final Logger log = LoggerFactory.getLogger(StreamTarget.class);
    /* max outstanding unack'd writes */
    private static final int MAX_SEND_BUFFER = Parameter.intValue("meshy.send.buffer", 5) * 1024 * 1024;
    /* max number of concurrently open streams */
    static final int MAX_OPEN_STREAMS = Parameter.intValue("meshy.stream.maxopen", 1000);
    /* create N sender threads ? */
    static final int SENDER_THREADS = Parameter.intValue("meshy.senders", 1);

    protected static final ConcurrentHashMap<String, VirtualFileReference> openStreamMap = new ConcurrentHashMap<>();

    public static void debugOpenTargets() {
        for (Map.Entry<String, VirtualFileReference> e : openStreamMap.entrySet()) {
            log.info("OPEN: {} = {}", e.getKey(), e.getValue());
        }
    }

    static final LinkedBlockingQueue<Runnable> senderQueue = new LinkedBlockingQueue<>(MAX_OPEN_STREAMS - SENDER_THREADS);

    private static final ExecutorService senderPool = MoreExecutors
            .getExitingExecutorService(new ThreadPoolExecutor(SENDER_THREADS, SENDER_THREADS, 0L, TimeUnit.MILLISECONDS,
                    senderQueue,
                    new ThreadFactoryBuilder().setNameFormat("sender-%d").build()), 1, TimeUnit.SECONDS);

    private static final ExecutorService inMemorySenderPool = MoreExecutors
            .getExitingExecutorService(new ThreadPoolExecutor(SENDER_THREADS, SENDER_THREADS, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new ThreadFactoryBuilder().setNameFormat("inMemorySender-%d").build()), 1, TimeUnit.SECONDS);

    //closed is only set to true in modeClose
    private final AtomicBoolean closed = new AtomicBoolean(false);
    //streamComplete is only set to true in streamComplete
    private final AtomicBoolean streamComplete = new AtomicBoolean(false);
    private final AtomicInteger sendRemain = new AtomicInteger(0);
    private final AtomicBoolean queueState = new AtomicBoolean(false);

    private int maxSend = 0;
    private VirtualFileInput fileIn = null;
    private StreamSource remoteSource = null;
    private String fileName;
    private int recvMore = 0;
    private int sentBytes = 0;

    @Override
    protected Objects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
                .add("closed", closed)
                .add("streamComplete", streamComplete)
                .add("sendRemain", sendRemain)
                .add("queueState", queueState)
                .add("fileIn", fileIn)
                .add("remoteSource", remoteSource)
                .add("fileName", fileName)
                .add("recvMore", recvMore)
                .add("sentBytes", sentBytes)
                .add("maxSend", maxSend);
    }

    private void sendFail(String message) {
        if (log.isDebugEnabled()) {
            log.debug(this + " sendFail " + message);
        }
        send(Bytes.cat(new byte[]{StreamService.MODE_FAIL}, Bytes.toBytes(message)));
        sendComplete();
    }

    /**
     * - Called by modeClose, receive when doing rerouting, and sendFail
     * - Sends a sendComplete flag to source (null bytes)
     * - Cleans up target session / removes handler
     */
    @Override
    public boolean sendComplete() {
        boolean out = super.sendComplete();
        getChannelState().removeHandlerOnComplete(this);
        return out;
    }

    @Override
    public void channelClosed() {
        if (remoteSource != null) {
            if (log.isDebugEnabled()) {
                log.debug(this + " closing remote on channel down");
            }
            remoteSource.requestClose();
        }
    }

    private static VirtualFileReference locateFile(VirtualFileSystem vfs, String path) {
        if (log.isTraceEnabled()) {
            log.trace("locate " + vfs + " --> " + path);
        }
        String tokens[] = vfs.tokenizePath(path);
        VirtualFileReference fileRef = vfs.getFileRoot();
        for (String token : tokens) {
            fileRef = fileRef.getFile(token);
            if (fileRef == null) {
                if (log.isTraceEnabled()) {
                    log.trace("no file " + vfs + " --> " + path + " --> " + fileRef);
                }
                return null;
            }
        }
        if (log.isTraceEnabled()) {
            log.trace("located " + vfs + " --> " + path + " --> " + fileRef);
        }
        return fileRef;
    }

    @Override
    public void receive(int length, ByteBuf buffer) throws Exception {
        byte[] data = Meshy.getBytes(length, buffer);
        if (remoteSource != null) {
            if (log.isTraceEnabled()) {
                log.trace(this + " recv proxy to " + remoteSource);
            }
            remoteSource.send(data);
            return;
        }
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        int mode = in.read();
        if (log.isTraceEnabled()) {
            log.trace(this + " recv mode=" + mode + " len=" + length);
        }
        switch (mode) {
            case StreamService.MODE_START:
            case StreamService.MODE_START_2:
                String nodeUuid = Bytes.readString(in);
                fileName = Bytes.readString(in);
                maxSend = Bytes.readInt(in);
                if (log.isTraceEnabled()) {
                    log.trace(this + " start uuid=" + nodeUuid + " file=" + fileName + " max=" + maxSend);
                }
                if (maxSend <= 0) {
                    sendFail("invalid buffer size: " + maxSend);
                    return;
                }
                if (StreamService.openStreams.count() > MAX_OPEN_STREAMS) {
                    log.warn("max open files: rejecting " + fileName);
                    sendFail(StreamService.ERROR_EXCEED_OPEN);
                    return;
                }
                Map<String, String> params = null;
                if (mode == StreamService.MODE_START_2) {
                    int count = Bytes.readInt(in);
                    params = new HashMap<>();
                    while (count-- > 0) {
                        String key = Bytes.readString(in);
                        String val = Bytes.readString(in);
                        params.put(key, val);
                    }
                }
                if (nodeUuid.equals(getChannelMaster().getUUID())) {
                    VirtualFileReference ref = null;
                    for (VirtualFileSystem vfs : getChannelMaster().getFileSystems()) {
                        ref = locateFile(vfs, fileName);
                        if (ref != null) {
                            fileIn = ref.getInput(params);
                            if (fileIn == null && log.isDebugEnabled()) {
                                log.debug("null file :: vfs=" + vfs.getClass().getName() + " ref=" + ref + " param=" + params + " fileIn=" + fileIn);
                            }
                            break;
                        }
                    }
                    if (ref == null || fileIn == null) {
                        sendFail("No Such File: " + fileName);
                        return;
                    }
                    StreamService.newStreamMeter.mark();
                    StreamService.openStreams.inc();
                    StreamService.newOpenStreams.incrementAndGet();
                    openStreamMap.put(fileName, ref);
                    if (log.isTraceEnabled()) {
                        log.trace(this + " start local=" + fileIn);
                    }
                } else {
                    remoteSource = new StreamSource(getChannelMaster(), nodeUuid, nodeUuid, fileName, params, maxSend) {
                        @Override
                        public void receive(int length, ByteBuf buffer) throws Exception {
                            if (StreamService.DIRECT_COPY) {
                                StreamTarget.this.send(buffer, length);
                            } else {
                                StreamTarget.this.send(Meshy.getBytes(length, buffer));
                            }
                        }

                        @Override
                        public void receiveComplete() {
                            StreamTarget.this.sendComplete();
                        }
                    };
                    if (log.isTraceEnabled()) {
                        log.trace(this + " start remote=" + remoteSource + " #" + remoteSource.getPeerCount());
                    }
                    if (remoteSource.getPeerCount() == 0) {
                        sendFail("no matching peers");
                        remoteSource = null;
                    }
                    return;
                }
                break;
            case StreamService.MODE_MORE:
                if (log.isTraceEnabled()) {
                    log.trace(this + " more request");
                }
                int current = sendRemain.addAndGet(maxSend);
                if (current - maxSend <= 0) {
                    if (queueState.compareAndSet(false, true)) {
                        scheduleForSending();
                    }
                }
                recvMore++;
                break;
            case StreamService.MODE_CLOSE:
                if (log.isTraceEnabled()) {
                    log.trace(this + " close request");
                }
                modeClose();
                break;
            default:
                log.warn(getChannelMaster().getUUID() + " target unknown mode: " + mode);
                break;
        }
    }

    @Override
    public void receiveComplete() throws Exception {
        if (remoteSource != null) {
            remoteSource.sendComplete();
        } else {
            complete();
        }
    }

    /*
    - Called by doSendMore when the file reports EOF and by receive when the source sends a MODE_CLOSE
    - Sends a MODE_CLOSE to source
    - Calls sendComplete and streamComplete()
    - Sets closed to true
     */
    private void modeClose() throws Exception {
        if (log.isDebugEnabled()) {
            log.debug(this + " close " + fileIn + " remote=" + remoteSource + " closed=" + closed);
        }
        if (closed.compareAndSet(false, true)) {
            if (log.isDebugEnabled()) {
                log.debug(this + " sending close");
            }
            send(new byte[]{StreamService.MODE_CLOSE});
            sendComplete();
            complete();
        }
    }

    /*
     - Called by modeClose and by receiveComplete
     - Closes the file and reduces open stream metrics
     - Sets streamComplete to true
      */
    private void complete() throws Exception {
        if (log.isDebugEnabled()) {
            log.debug(this + " streamComplete " + fileIn + " remote=" + remoteSource + " streamComplete=" + streamComplete);
        }
        if (streamComplete.compareAndSet(false, true) && fileIn != null) {
            if (log.isDebugEnabled()) {
                log.debug(this + " close file on streamComplete");
            }
            fileIn.close();
            StreamService.closedStreams.incrementAndGet();
            StreamService.openStreams.dec();
            openStreamMap.remove(fileName);
        }
    }

    private boolean maybeDropSend() {
        if (sendRemain.get() <= 0) {
            queueState.set(false);
            if (sendRemain.get() <= 0 || !queueState.compareAndSet(false, true)) {
                return true;
            }
        }
        return closed.get() || streamComplete.get() || fileIn == null;
    }

    @Override
    public void run() {
        int sentBytes = 0;
        int lastSent = 0;
        while (sentBytes < maxSend) {
            try {
                long mark = System.currentTimeMillis();
                StreamService.totalReads.incrementAndGet();
                if (lastSent > 0) {
                    StreamService.seqReads.incrementAndGet();
                }
                lastSent = doSendMore(this);
                if (lastSent <= 0) {
                    break;
                }
                sentBytes += lastSent;
                StreamService.sendWaiting.addAndGet(lastSent);
                StreamService.readWaitTime.addAndGet((int) (System.currentTimeMillis() - mark));
            /* TODO this sucks, but works */
                if (StreamService.sendWaiting.get() > MAX_SEND_BUFFER) {
                    if (log.isTraceEnabled()) {
                        log.trace(this + " sleeping " + StreamService.sendWaiting + " > " + MAX_SEND_BUFFER);
                    }
                    Thread.sleep(10);
                    StreamService.sleeps.incrementAndGet();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                sendFail(ex.toString());
                return;
            }
        }
        if (lastSent >= 0) {
            if (!maybeDropSend()) {
                scheduleForSending();
            }
        }
    }

    private void scheduleForSending() {
        if (log.isTraceEnabled()) {
            log.trace(this + " scheduleTask");
        }
        // Paths that start with meshy are usually in-memory stat collections
        if (fileName.startsWith("/meshy/")) {
            inMemorySenderPool.execute(this);
        } else {
            senderPool.execute(this);
        }
        if (log.isTraceEnabled()) {
            log.trace(this + " scheduled ");
        }
    }

    private int doSendMore(SendWatcher sender) {
        if (log.isTraceEnabled()) {
            log.trace(this + " sendMore for " + sender + " rem=" + sendRemain + " closed=" + closed + " streamComplete=" + streamComplete + " fileIn=" + fileIn);
        }
        try {
            if (maybeDropSend()) {
                if (StreamService.LOG_DROP_MORE || log.isDebugEnabled()) {
                    log.debug(this + " sendMore drop request sr=" + sendRemain + ", cl=" + closed + ", co=" + streamComplete + ", fi=" + fileIn);
                }
                return -1;
            }
            byte next[] = fileIn.nextBytes(StreamService.READ_WAIT);
            if (next != null) {
                if (log.isTraceEnabled()) {
                    log.trace(this + " send add read=" + next.length);
                }
                StreamService.readBytes.addAndGet(next.length);
                ByteBuf buf = getSendBuffer(next.length + StreamService.STREAM_BYTE_OVERHEAD);
                buf.writeByte(StreamService.MODE_MORE);
                buf.writeBytes(next);
                int bytesSent = send(buf, sender);
                sendRemain.addAndGet(-bytesSent);
                sentBytes += bytesSent;
                if (log.isTraceEnabled()) {
                    log.trace(this + " read read=" + next.length + " sendRemain=" + sendRemain);
                }
                return bytesSent;
            } else if (fileIn.isEOF()) {
                if (log.isTraceEnabled()) {
                    log.trace(this + " read close on null. sendRemain=" + sendRemain);
                }
                modeClose();
                return -1;
            } else {
                if (log.isDebugEnabled()) {
                    log.debug(this + " read timeout. sendRemain=" + sendRemain);
                }
                return 0;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return -1;
        }
    }

    @Override
    public void sendFinished(int bytes) {
        if (log.isTraceEnabled()) {
            log.trace(this + " send finished " + bytes);
        }
        StreamService.sendWaiting.addAndGet(-bytes);
    }
}
