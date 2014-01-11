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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.meshy.service.message.InternalHandler;
import com.addthis.meshy.service.message.MessageFileSystem;
import com.addthis.meshy.service.peer.PeerSource;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.nio.NioServerSocketChannel;


public class MeshyServer extends Meshy {

    protected static final Logger log = LoggerFactory.getLogger(MeshyServer.class);

    private static final boolean autoMesh = Parameter.boolValue("meshy.autoMesh", false);
    private static final boolean allowPeerLocal = Parameter.boolValue("meshy.peer.local", true); // permit peering in local VM
    private static final int autoMeshTimeout = Parameter.intValue("meshy.autoMeshTimeout", 60000);
    private static final ArrayList<byte[]> vmLocalNet = new ArrayList<>(3);
    private static final HashMap<String, VirtualFileSystem[]> vfsCache = new HashMap<>();
    private static final MessageFileSystem messageFileSystem = new MessageFileSystem();
    private static final Counter peerCountMetric = Metrics.newCounter(Meshy.class, "peerCount");
    private static final HashSet<String> blockedPeers = new HashSet<>();

    static {
        /* collect local valid interfaces for fast lookup */
        try {
            Enumeration<NetworkInterface> enNet = NetworkInterface.getNetworkInterfaces();
            while (enNet.hasMoreElements()) {
                NetworkInterface net = enNet.nextElement();
                if (net.isLoopback() || net.isPointToPoint() || !net.isUp()) {
                    continue;
                }
                Enumeration<InetAddress> enAddr = net.getInetAddresses();
                while (enAddr.hasMoreElements()) {
                    byte addr[] = enAddr.nextElement().getAddress();
                    /* only allow IPV4 at the moment */
                    if (addr.length == 4) {
                        vmLocalNet.add(addr);
                    }
                }
            }
            log.info("local net: {}", vmLocalNet);

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

    }

    public static void resetFileSystems() {
        vfsCache.clear();
    }

    /**
     * cache for use of multiple servers in one VM
     */
    protected static VirtualFileSystem[] loadFileSystems(File rootDir) {
        String cacheKey = rootDir != null ? rootDir.getAbsolutePath() : ".";
        VirtualFileSystem[] vfsList = vfsCache.get(cacheKey);
        if (vfsList == null) {
            LinkedList<VirtualFileSystem> load = new LinkedList<>();
            load.add(messageFileSystem);
            if (rootDir != null) {
                load.add(new LocalFileSystem(rootDir));
            }
            String registerVMs = Parameter.value("meshy.vms");
            if (registerVMs != null) {
                for (String vm : Strings.splitArray(registerVMs, ",")) {
                    try {
                        load.add((VirtualFileSystem) (Class.forName(vm).newInstance()));
                    } catch (Exception t) {
                        log.error("failure loading VM " + vm, t);
                    }
                }
            }
            vfsList = load.toArray(new VirtualFileSystem[load.size()]);
            vfsCache.put(cacheKey, vfsList);
        }
        return vfsList;
    }

    final int serverPort;
    final MeshyServerGroup group;
    private final File rootDir;
    private final VirtualFileSystem filesystems[];
    private final List<Channel> serverChannel;
    private final String serverUuid;

    /**
     * This guard is used to ensure the close() method
     * is invoked exactly once.
     */
    private final AtomicBoolean closeGuard = new AtomicBoolean(false);

    /**
     * Two types of threads can call the close() method.
     * One type is an application thread which is a user level thread.
     * The other type is the shutdown thread which is a JVM thread.
     * <p/>
     * If the user level thread begins the close() method and the
     * JVM begins to terminate then it is possible for the JVM to terminate
     * before the user level thread has completed the close() method.
     * <p/>
     * This semaphore is used to block the shutdown thread. The goal is to
     * suspend the shutdown thread (and JVM termination) until the user level
     * thread has completed and increments the semaphore.
     * <p/>
     * Alternatively the user level thread may block while the shutdown thread completes
     * the close() method. This code path is benign as the shutdown thread
     * will not be prematurely terminated.
     * <p/>
     * Both 'closeSemaphore' and 'closeGuard' are needed to work in concert.
     */
    private final Semaphore closeSemaphore = new Semaphore(1);

    private final Thread shutdownThread;

    private InetSocketAddress serverLocal;
    private String serverNetIf;
    // TODO: revisit this one's visibility
    boolean initialized;

    /**
     * server
     */
    public MeshyServer(int port) throws IOException {
        this(port, new File("."));
    }

    public MeshyServer(int port, File rootDir) throws IOException {
        this(port, rootDir, null, new MeshyServerGroup());
    }

    public MeshyServer(final int port, final File rootDir, String netif[], final MeshyServerGroup group) throws IOException {
        super();
        this.group = group;
        this.serverPort = port;
        this.rootDir = rootDir;
        this.filesystems = loadFileSystems(rootDir);
        group.join(this);
        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30000)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new MeshyChannelnitializer(MeshyServer.this));

        serverChannel = new LinkedList<>();
        /* bind to one or more interfaces, if supplied, otherwise all */
        if (netif == null || netif.length == 0) {
            serverLocal = new InetSocketAddress(port);
            serverChannel.add(bootstrap.bind(serverLocal).channel()); //async
        } else {
            for (String net : netif) {
                NetworkInterface nicif = NetworkInterface.getByName(net);
                if (nicif == null) {
                    log.warn("missing speficied NIC: {}", net);
                    continue;
                }
                for (InterfaceAddress addr : nicif.getInterfaceAddresses()) {
                    InetAddress inAddr = addr.getAddress();
                    if (inAddr.getAddress().length != 4) {
                        log.trace("skip non-ipV4 address: {}", inAddr);
                        continue;
                    }
                    serverLocal = new InetSocketAddress(inAddr, port);
                    serverChannel.add(bootstrap.bind(serverLocal).channel()); //async
                }
                serverNetIf = net;
            }
        }
        if (autoMesh) {
            startAutoMesh(serverPort, autoMeshTimeout);
        }
        serverUuid = super.getUUID() + "-" + port + (serverNetIf != null ? "-" + serverNetIf : "");
        log.info("server [" + getUUID() + "] on " + port + " @ " + rootDir);
        shutdownThread = new Thread() {
            public void run() {
                log.info("Running shutdown hook..");
                close();
                log.info("Shutdown hook complete.");
            }
        };
        Runtime.getRuntime().addShutdownHook(shutdownThread);
        messageFileSystem.addPath("/meshy/" + getUUID() + "/stats", new InternalHandler() {
            @Override
            public byte[] handleMessageRequest(String fileName, Map<String, String> options) {
                StringBuilder sb = new StringBuilder();
                for (String line : group.getLastStats()) {
                    sb.append(line);
                    sb.append("\n");
                }
                return Bytes.toBytes(sb.toString());
            }
        });
        messageFileSystem.addPath("/meshy/statsMap", new InternalHandler() {
            @Override
            public byte[] handleMessageRequest(String fileName, Map<String, String> options) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                Map<String, Integer> stats = group.getLastStatsMap();
                try {
                    Bytes.writeInt(stats.size(), out);
                    for (Map.Entry<String, Integer> e : stats.entrySet()) {
                        Bytes.writeString(e.getKey(), out);
                        Bytes.writeInt(e.getValue(), out);
                    }
                } catch (IOException e) {
                    //ByteArrayOutputStreams do not throw IOExceptions
                }
                return out.toByteArray();
            }
        });
        initialized = true;
    }

    @Override
    public String toString() {
        return "MS:{" + serverPort + "," + getUUID() + ",all=" + getChannelCount() + ",sm=" + getPeeredCount() + "}";
    }

    public File getRootDir() {
        return rootDir;
    }

    public MeshyServer[] getMembers() {
        return group.getMembers();
    }

    @Override
    public String getUUID() {
        return serverUuid;
    }

    @Override
    protected void connectChannel(Channel channel, ChannelState channelState) {
        super.connectChannel(channel, channelState);
        /* servers peer with other servers once a channel comes up */
        // assign unique id (local or remote inferred)
        channelState.setName("temp-uuid-" + nextSession.incrementAndGet());
        InetSocketAddress address = (InetSocketAddress) channel.remoteAddress();
        if (needsPeering.remove(address)) {
            if (log.isDebugEnabled()) {
                log.debug(MeshyServer.this + " >>> starting peering with " + address);
            }
            new PeerSource(this, channelState.getName());
        }
    }

    @Override
    public void close() {
        closeSemaphore.acquireUninterruptibly();
        try {
            if (!closeGuard.getAndSet(true)) {
                log.debug(this + " exiting");
                super.close();
                if (serverChannel != null) {
                    for (Channel ch : serverChannel) {
                        ch.close().awaitUninterruptibly();
                    }
                }
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownThread);
                } catch (IllegalStateException ex) {
                    // the JVM is shutting down
                }
            }
        } finally {
            closeSemaphore.release();
        }
    }

    public int getLocalPort() {
        return serverPort;
    }

    public String getNetIf() {
        return serverNetIf;
    }

    public InetSocketAddress getLocalAddress() {
        return serverLocal;
    }

    public VirtualFileSystem[] getFileSystems() {
        return filesystems;
    }

    /**
     * blocking call.  used by main() command-line forced peering
     */
    public void connectPeer(InetSocketAddress address) {
        ChannelFuture future = connectToPeer(null, address);
        if (future == null) {
            log.info(MeshyServer.this + " peer connect returned null future to " + address);
            return;
        }
        /* wait for connection to complete */
        future.awaitUninterruptibly();
        if (!future.isSuccess()) {
            log.warn(MeshyServer.this + " peer connect fail to " + address);
        }
    }

    public void blockPeer(final String peerUuid) {
        synchronized (blockedPeers) {
            blockedPeers.add(peerUuid);
        }
        dropPeer(peerUuid);
    }

    public void dropPeer(final String peerUuid) {
        synchronized (connectedChannels) {
            for (ChannelState state : connectedChannels) {
                if (state.getName().equals(peerUuid)) {
                    state.getChannel().close();
                }
            }
        }
    }

    /**
     * connects to a peer address.  if peerUuid is not know, method blocks
     */
    public ChannelFuture connectToPeer(final String peerUuid, final InetSocketAddress pAddr) {
        synchronized (blockedPeers) {
            if (peerUuid != null && blockedPeers.contains(peerUuid)) {
                return null;
            }
        }
        updateLastEventTime();
        if (log.isDebugEnabled()) {
            log.debug(MeshyServer.this + " request connect to " + peerUuid + " @ " + pAddr);
        }
        /* skip peering with self (and other meshes started in the same vm) */
        if (peerUuid != null && group.hasUuid(peerUuid)) {
            if (log.isDebugEnabled()) {
                log.debug(this + " skipping " + peerUuid + " .. it's me");
            }
            return null;
        }
        /* skip peering with self */
        try {
            for (Enumeration<NetworkInterface> eni = NetworkInterface.getNetworkInterfaces(); eni.hasMoreElements(); ) {
                for (Enumeration<InetAddress> eaddr = eni.nextElement().getInetAddresses(); eaddr.hasMoreElements(); ) {
                    InetAddress addr = eaddr.nextElement();
                    if (addr.equals(pAddr.getAddress()) && pAddr.getPort() == serverPort) {
                        if (log.isDebugEnabled()) {
                            log.debug(this + " skipping myself " + addr + " : " + serverPort);
                        }
                        return null;
                    }
                }
            }
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
        /* check for local peering */
        if (!allowPeerLocal) {
            byte peerAddr[] = pAddr.getAddress().getAddress();
            for (byte addr[] : vmLocalNet) {
                if (Bytes.equals(peerAddr, addr)) {
                    log.info("peer reject local " + pAddr);
                    return null;
                }
            }
        }
        /* skip peering again */
        if (log.isDebugEnabled()) {
            log.debug(this + " peer.check (uuid=" + peerUuid + " addr=" + pAddr + ")");
        }
        synchronized (connectedChannels) {
            for (ChannelState channelState : connectedChannels) {
                if (log.isTraceEnabled()) {
                    log.trace(" --> state=" + channelState);
                }
                if (peerUuid != null && channelState.getName() != null && channelState.getName().equals(peerUuid)) {
                    if (log.isTraceEnabled()) {
                        log.trace(this + " 1.peer.uuid " + peerUuid + " already connected");
                    }
                    return null;
                }
                InetSocketAddress remoteAddr = channelState.getRemoteAddress();
                if (remoteAddr != null && remoteAddr.equals(pAddr)) {
                    if (log.isTraceEnabled()) {
                        log.trace(this + " 2.peer.addr " + pAddr + " already connected");
                    }
                    return null;
                }
            }
            /* already actively peering with this uuid */
            if (peerUuid != null && !inPeering.add(peerUuid)) {
                if (log.isTraceEnabled()) {
                    log.trace(MeshyServer.this + " skip already peering " + peerUuid);
                }
                return null;
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(this + " connecting to " + peerUuid + " @ " + pAddr);
        }
        /* allows us to know if *we* initiated the connection and thus must also initiate the peering */
        needsPeering.add(pAddr);
        /* connect to peer */
        return connect(pAddr);
    }

    Stats getStats() {
        return new Stats(this);
    }

    /**
     * for cmd-line stats output
     */
    public static class Stats {

        final int bin;
        final int bout;
        final int channelCount;
        final int peerCount;

        Stats(MeshyServer server) {
            bin = server.getAndClearRecv();
            bout = server.getAndClearSent();
            channelCount = server.getChannelCount();
            peerCount = server.getPeeredCount();
            peerCountMetric.clear();
            peerCountMetric.inc(server.getPeeredCount());
        }
    }

    /**
     * thread that uses UDP to auto-mesh server instances
     *
     * @param port
     * @param timeout
     */
    private void startAutoMesh(final int port, final int timeout) {
        // create UDP broadcast / listener
        Thread autoMeshThread = new Thread(new AutoMesher(this, port, timeout), "AutoMesh thread");
        autoMeshThread.setDaemon(true);
        autoMeshThread.start();
    }

}
