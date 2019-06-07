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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.basis.util.LessBytes;
import com.addthis.basis.util.Parameter;

import com.addthis.meshy.service.message.MessageFileSystem;
import com.addthis.meshy.service.peer.PeerSource;

import com.google.common.base.MoreObjects;
import com.google.common.base.Splitter;
import com.google.common.hash.Hashing;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;

import static java.nio.charset.StandardCharsets.UTF_8;


public class MeshyServer extends Meshy {
    protected static final Logger log = LoggerFactory.getLogger(MeshyServer.class);

    private static final boolean autoMesh = Parameter.boolValue("meshy.autoMesh", false);
    // permit peering in local VM
    private static final boolean allowPeerLocal = Parameter.boolValue("meshy.peer.local", true);
    private static final int autoMeshTimeout = Parameter.intValue("meshy.autoMeshTimeout", 60000);

    static final Counter peerCountMetric = Metrics.newCounter(Meshy.class, "peerCount");

    private static final ArrayList<byte[]> vmLocalNet = new ArrayList<>(3);
    private static final HashMap<String, VirtualFileSystem[]> vfsCache = new HashMap<>();
    private static final HashSet<String> blockedPeers = new HashSet<>();

    public static final MessageFileSystem messageFileSystem = new MessageFileSystem();

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
                    byte[] addr = enAddr.nextElement().getAddress();
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
                for (String vm : Splitter.on(",").omitEmptyStrings().trimResults().split(registerVMs)) {
                    try {
                        load.add((VirtualFileSystem) Class.forName(vm).newInstance());
                    } catch (Throwable t) {
                        log.error("failure loading VM {}", vm, t);
                    }
                }
            }
            vfsList = load.toArray(new VirtualFileSystem[load.size()]);
            vfsCache.put(cacheKey, vfsList);
        }
        return vfsList;
    }

    private final int serverPort;
    private final File rootDir;
    private final VirtualFileSystem[] filesystems;
    private final EventLoopGroup bossGroup;
    private final String serverUuid;
    private final MeshyServerGroup group;
    private final AtomicInteger serverPeers;

    private final Promise<?> closeFuture;

    private final InetSocketAddress serverLocal;
    private final NetworkInterface serverNetIf;

    public MeshyServer(int port) throws IOException {
        this(port, new File("."), null);
    }

    public MeshyServer(int port, File rootDir, List<InetSocketAddress> peers) throws IOException {
        this(port, rootDir, null, new MeshyServerGroup(), peers);
    }

    public MeshyServer(final int port, final File rootDir, @Nullable String[] netif, final MeshyServerGroup group, List<InetSocketAddress> peers)
            throws IOException {
        super();
        this.group = group;
        this.rootDir = rootDir;
        this.filesystems = loadFileSystems(rootDir);
        this.serverPeers = new AtomicInteger(0);
        bossGroup = new NioEventLoopGroup(1);
        ServerBootstrap bootstrap = new ServerBootstrap()
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30000)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, HIGH_WATERMARK)
                .childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, LOW_WATERMARK)
                .channel(NioServerSocketChannel.class)
                .group(bossGroup, workerGroup)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(final NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new ChannelState(MeshyServer.this, ch));
                    }
                });
        /* bind to one or more interfaces, if supplied, otherwise all */
        if ((netif == null) || (netif.length == 0)) {
            ServerSocketChannel serverChannel =
                    (ServerSocketChannel) bootstrap.bind(new InetSocketAddress(port)).syncUninterruptibly().channel();
            serverLocal = serverChannel.localAddress();
        } else {
            InetSocketAddress primaryServerLocal = null;
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
                    ServerSocketChannel serverChannel =
                            (ServerSocketChannel) bootstrap.bind(new InetSocketAddress(inAddr, port))
                                                           .syncUninterruptibly()
                                                           .channel();
                    if (primaryServerLocal != null) {
                        log.info("server [{}-*] binding to extra address: {}", super.getUUID(), primaryServerLocal);
                    }
                    primaryServerLocal = serverChannel.localAddress();
                }
            }
            if (primaryServerLocal == null) {
                throw new IllegalArgumentException("no valid interface / port specified");
            }
            serverLocal = primaryServerLocal;
        }
        this.serverNetIf = NetworkInterface.getByInetAddress(serverLocal.getAddress());
        this.serverPort = serverLocal.getPort();
        if (serverNetIf != null) {
            serverUuid = super.getUUID() + "-" + serverPort + "-" + serverNetIf.getName();
        } else {
            serverUuid = super.getUUID() + "-" + serverPort;
        }
        log.info("server [{}] on {} @ {}", getUUID(), serverLocal, rootDir);
        closeFuture = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);
        workerGroup.terminationFuture().addListener((Future<Object> workerFuture) -> {
            bossGroup.terminationFuture().addListener((Future<Object> bossFuture) -> {
                if (!workerFuture.isSuccess()) {
                    closeFuture.tryFailure(workerFuture.cause());
                } else if (!bossFuture.isSuccess()) {
                    closeFuture.tryFailure(bossFuture.cause());
                } else {
                    closeFuture.trySuccess(null);
                }
            });
        });
        addMessageFileSystemPaths();
        group.join(this);
        if (autoMesh) {
            startAutoMesh(serverPort, autoMeshTimeout);
        } else if (peers != null && peers.size() > 0){
            startAutoConnectToPeers(peers, autoMeshTimeout);
        }
    }

    private void addMessageFileSystemPaths() {
        messageFileSystem.addPath("/meshy/" + getUUID() + "/stats", (fileName, options) -> {
            StringBuilder sb = new StringBuilder();
            for (String line : group.getLastStats()) {
                sb.append(line);
                sb.append("\n");
            }
            return sb.toString().getBytes(UTF_8);
        });
        messageFileSystem.addPath("/meshy/statsMap", (fileName, options) -> {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Map<String, Integer> stats = group.getLastStatsMap();
            try {
                LessBytes.writeInt(stats.size(), out);
                for (Map.Entry<String, Integer> e : stats.entrySet()) {
                    LessBytes.writeString(e.getKey(), out);
                    LessBytes.writeInt(e.getValue(), out);
                }
            } catch (IOException e) {
                //ByteArrayOutputStreams do not throw IOExceptions
            }
            return out.toByteArray();
        });
        messageFileSystem.addPath("/meshy/host/ban", (fileName, options) -> {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            String hostName = options.get("host");
            synchronized (blockedPeers) {
                try {
                    if (hostName == null) {
                        for (String peer : blockedPeers) {
                            LessBytes.writeString(peer + "\n", out);
                        }
                    } else {
                        if (blockedPeers.contains(hostName)) {
                            LessBytes.writeString(hostName + " already in blocked peers\n", out);
                        } else {
                            LessBytes.writeString(hostName + " added to blocked peers\n", out);
                        }
                        if (dropPeer(hostName)) {
                            LessBytes.writeString(hostName + " connection closed (async)\n", out);
                        } else {
                            LessBytes.writeString(hostName + " connection not found\n", out);
                        }
                    }
                } catch (IOException ignored) {
                }
            }
            return out.toByteArray();
        });
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
    protected void channelConnected(Channel channel, ChannelState channelState) {
        super.channelConnected(channel, channelState);
        /* servers peer with other servers once a channel comes up */
        // assign unique id (local or remote inferred)
        channelState.setName("temp-uuid-" + nextSession.incrementAndGet());
        InetSocketAddress address = (InetSocketAddress) channel.remoteAddress();
        if (channel.parent() == null) {
            log.debug("{} >>> starting peering with {}", MeshyServer.this, address);
            new PeerSource(this, channelState.getName());
        }
    }

    @Override
    protected void channelClosed(Channel channel, ChannelState channelState) {
        super.channelClosed(channel, channelState);
        if (channelState.getRemoteAddress() != null) {
            serverPeers.decrementAndGet();
        }
    }

    @Override
    public void close() {
        log.debug("{} exiting", this);
        closeAsync().awaitUninterruptibly();
    }

    @Override public Future<?> closeAsync() {
        bossGroup.shutdownGracefully(Meshy.QUIET_PERIOD, Meshy.SHUTDOWN_TIMEOUT, TimeUnit.SECONDS);
        super.closeAsync();
        return closeFuture.awaitUninterruptibly();
    }

    public Future<?> closeFuture() {
        return closeFuture;
    }

    public int getLocalPort() {
        return serverPort;
    }

    public NetworkInterface getNetIf() {
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
            log.info("{} peer connect returned null future to {}", MeshyServer.this, address);
            return;
        }
        /* wait for connection to complete */
        future.awaitUninterruptibly();
        if (!future.isSuccess()) {
            log.warn("{} peer connect fail to {}", MeshyServer.this, address);
        }
    }

    public boolean blockPeer(final String peerUuid) {
        synchronized (blockedPeers) {
            blockedPeers.add(peerUuid);
        }
        return dropPeer(peerUuid);
    }

    public boolean dropPeer(final String peerUuid) {
        boolean hitAtLeastOnce = false;
        synchronized (connectedChannels) {
            for (ChannelState state : connectedChannels) {
                if (state.getName().equals(peerUuid) && state.getChannel().isOpen()) {
                    state.getChannel().close();
                    hitAtLeastOnce = true;
                }
            }
        }
        return hitAtLeastOnce;
    }

    /**
     * connects to a peer address.  if peerUuid is not know, method blocks
     */
    @Nullable public ChannelFuture connectToPeer(@Nullable final String peerUuid, final InetSocketAddress pAddr) {
        synchronized (blockedPeers) {
            if (peerUuid != null && blockedPeers.contains(peerUuid)) {
                return null;
            }
        }
        updateLastEventTime();
        log.debug("{} request connect to {} @ {}", MeshyServer.this, peerUuid, pAddr);
        /* skip peering with self (and other meshes started in the same vm) */
        if (peerUuid != null && group.hasUuid(peerUuid)) {
            log.debug("{} skipping {} .. it's me", this, peerUuid);
            return null;
        }
        /* skip peering with self */
        try {
            for (Enumeration<NetworkInterface> eni = NetworkInterface.getNetworkInterfaces(); eni.hasMoreElements(); ) {
                for (Enumeration<InetAddress> eaddr = eni.nextElement().getInetAddresses(); eaddr.hasMoreElements(); ) {
                    InetAddress addr = eaddr.nextElement();
                    if (addr.equals(pAddr.getAddress()) && pAddr.getPort() == serverPort) {
                        log.debug("{} skipping myself {} : {}", this, addr, serverPort);
                        return null;
                    }
                }
            }
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
        /* check for local peering */
        if (!allowPeerLocal) {
            byte[] peerAddr = pAddr.getAddress().getAddress();
            for (byte[] addr : vmLocalNet) {
                if (LessBytes.equals(peerAddr, addr)) {
                    log.info("peer reject local {}", pAddr);
                    return null;
                }
            }
        }
        /* skip peering again */
        log.debug("{} peer.check (uuid={} addr={})", this, peerUuid, pAddr);
        synchronized (connectedChannels) {
            for (ChannelState channelState : connectedChannels) {
                log.trace(" --> state={}", channelState);
                if (peerUuid != null && channelState.getName() != null && channelState.getName().equals(peerUuid)) {
                    log.trace("{} 1.peer.uuid {} already connected", this, peerUuid);
                    return null;
                }
                InetSocketAddress remoteAddr = channelState.getRemoteAddress();
                if (remoteAddr != null && remoteAddr.equals(pAddr)) {
                    log.trace("{} 2.peer.addr {} already connected", this, pAddr);
                    return null;
                }
            }
            /* already actively peering with this uuid */
            if (peerUuid != null && !inPeering.add(peerUuid)) {
                log.trace("{} skip already peering {}", MeshyServer.this, peerUuid);
                return null;
            }
        }
        log.debug("{} connecting to {} @ {}", this, peerUuid, pAddr);
        /* connect to peer */
        ChannelFuture connectionFuture = connect(pAddr);
        if (peerUuid != null) {
            connectionFuture.addListener(f -> {
                if (!f.isSuccess()) {
                    synchronized (connectedChannels) {
                        inPeering.remove(peerUuid);
                    }
                }
            });
        }
        return connectionFuture;
    }

    public boolean promoteToNamedServerPeer(ChannelState peerState, String newUuid, InetSocketAddress newAddr) {
        synchronized (connectedChannels) {
            for (ChannelState channelState : connectedChannels) {
                if (newUuid.equals(channelState.getName())) {
                    log.info("rejecting peerage for {} @ {} (to {} @ {}) because uuid matches existing {} for: {}",
                             peerState.getName(), peerState.getChannelRemoteAddress(), newUuid, newAddr,
                             channelState, this);
                    return false;
                }
                if (newAddr.equals(channelState.getRemoteAddress())) {
                    log.info("rejecting peerage for {} @ {} (to {} @ {}) because address matches existing {} for: {}",
                             peerState.getName(), peerState.getChannelRemoteAddress(), newUuid, newAddr,
                             channelState, this);
                    return false;
                }
            }
            log.info("promoting {} @ {} to named server peer as {} @ {} for: {}",
                     peerState.getName(), peerState.getChannelRemoteAddress(), newUuid, newAddr, this);
            peerState.setName(newUuid);
            peerState.setRemoteAddress(newAddr);
            serverPeers.incrementAndGet();
            return true;
        }
    }

    public int getServerPeerCount() {
        return serverPeers.get();
    }

    public boolean shouldBeConnector(String newUuid) {
        int myHash = Hashing.murmur3_32().hashUnencodedChars(getUUID()).asInt();
        int peerHash = Hashing.murmur3_32().hashUnencodedChars(newUuid).asInt();
        boolean useGreater = ((myHash ^ peerHash) & 0x1) == 0;

        int compareUuidStrings = getUUID().compareTo(newUuid);
        if (useGreater) {
            return compareUuidStrings > 0;
        } else {
            return compareUuidStrings < 0;
        }
    }

    ServerStats getStats() {
        return new ServerStats(this);
    }

    /** thread that uses UDP to auto-mesh server instances */
    private void startAutoMesh(final int port, final int timeout) {
        // create UDP broadcast / listener
        Thread t = new Thread(new AutoMeshTask(this, group, timeout, port),
                              "AutoMesh Peer Listener (port: " + port + ")");
        t.setDaemon(true);
        t.start();
    }

    private void startAutoConnectToPeers(List<InetSocketAddress> addresses, int timeout){
        Thread t= new Thread(new AutoConnectToPeersTask(this, addresses, timeout),
                "AutoConnectToPeers");
        t.setDaemon(true);
        t.start();
    }

    @Override public String toString() {
        return MoreObjects.toStringHelper(this)
                      .add("serverPort", serverPort)
                      .add("serverUuid", serverUuid)
                      .add("channelCount", getChannelCount())
                      .add("peeredCount", getServerPeerCount())
                      .toString();
    }
}
