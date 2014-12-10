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
package com.addthis.meshy.service.peer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import java.util.concurrent.LinkedBlockingQueue;

import com.addthis.basis.util.Bytes;

import com.addthis.meshy.ChannelState;
import com.addthis.meshy.MeshyConstants;
import com.addthis.meshy.MeshyServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * connect to a peer, receive from peer it's uuid and connection map
 */
public final class PeerService {

    static final Logger log = LoggerFactory.getLogger(PeerService.class);
    static final LinkedBlockingQueue<PeerTuple> peerQueue = new LinkedBlockingQueue<>();

    static final Thread peeringThread = new Thread() {
        {
            setName("peering finderQueue");
            setDaemon(true);
            start();
        }

        public void run() {
            while (true) {
                try {
                    peerQueue.take().connect();
                } catch (Exception e) {
                    log.warn("peer finderQueue exiting on error", e);
                    return;
                }
            }
        }
    };

    private PeerService() {
    }

    static boolean shouldEncode(InetSocketAddress sockAddr) {
        InetAddress addr = sockAddr.getAddress();
        return !(addr.isLoopbackAddress() || addr.isAnyLocalAddress());
    }

    public static void encodeAddress(InetSocketAddress addr, OutputStream out) throws IOException {
        Bytes.writeBytes(addr.getAddress().getAddress(), out);
        Bytes.writeInt(addr.getPort(), out);
    }

    public static InetSocketAddress decodeAddress(InputStream in) throws IOException {
        return new InetSocketAddress(InetAddress.getByAddress(Bytes.readBytes(in)), Bytes.readInt(in));
    }

    /**
     * send local peer uuid:port and list of peers to remote
     */
    public static byte[] encodePeer(MeshyServer master, ChannelState me) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Bytes.writeString(master.getUUID(), out);
            encodeAddress(master.getLocalAddress(), out);
            if (log.isDebugEnabled()) {
                log.debug("{} encode peer remote={}", master, me != null ? me.getChannel().remoteAddress() : "");
            }
            for (ChannelState channelState : master.getChannels(MeshyConstants.LINK_NAMED)) {
                Bytes.writeString(channelState.getName(), out);
                encodeAddress(channelState.getRemoteAddress(), out);
                if (log.isDebugEnabled()) {
                    log.debug("{} encoded {} @ {}", master, channelState.getName(),
                              channelState.getChannel().remoteAddress());
                }
            }
            for (MeshyServer member : master.getMembers()) {
                if (member != master && shouldEncode(member.getLocalAddress())) {
                    if (log.isTraceEnabled()) {
                        log.trace("encode MEMBER: {} / {}", member.getUUID(), member.getLocalAddress());
                    }
                    Bytes.writeString(member.getUUID(), out);
                    encodeAddress(member.getLocalAddress(), out);
                }
            }
            Bytes.writeString("", out);
            return out.toByteArray();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * receive peer's uuid:port and peer list then connect to ones that are new to us
     */
    public static void decodePeer(MeshyServer master, ChannelState me, InputStream in) {
        try {
            me.setName(Bytes.readString(in));
            InetSocketAddress sockAddr = decodeAddress(in);
            InetAddress inAddr = sockAddr.getAddress();
            /* if remote reports loopback or any-net, use peer ip addr + port */
            if (inAddr.isAnyLocalAddress() || inAddr.isLoopbackAddress()) {
                sockAddr = new InetSocketAddress(me.getChannelRemoteAddress().getAddress(), sockAddr.getPort());
            }
            me.setRemoteAddress(sockAddr);
            if (log.isDebugEnabled()) {
                log.debug("{} decode peer {}:{}:{}", master, me.getChannel().remoteAddress(), me.getName(),
                          me.getRemoteAddress());
            }
            while (true) {
                String peerUuid = Bytes.readString(in);
                if (peerUuid.isEmpty()) {
                    break;
                }
                InetSocketAddress peerAddress = decodeAddress(in);
                if (log.isDebugEnabled()) {
                    log.debug("{} decoded {} @ {}", master, peerUuid, peerAddress);
                }
                peerQueue.put(new PeerTuple(master, peerUuid, peerAddress));
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
