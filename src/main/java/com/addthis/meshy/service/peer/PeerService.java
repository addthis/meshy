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
package com.addthis.meshy.service.peer;

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
import com.addthis.meshy.util.ByteBufs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

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

    public static void encodeAddress(InetSocketAddress addr, ByteBuf out) throws IOException {
        ByteBufs.writeBytes(addr.getAddress().getAddress(), out);
        out.writeInt(addr.getPort());
    }

    public static InetSocketAddress decodeAddress(InputStream in) throws IOException {
        return new InetSocketAddress(InetAddress.getByAddress(Bytes.readBytes(in)), Bytes.readInt(in));
    }

    public static InetSocketAddress decodeAddress(ByteBuf in) throws IOException {
        return new InetSocketAddress(InetAddress.getByAddress(ByteBufs.readBytes(in)), in.readInt());
    }

    /**
     * send local peer uuid:port and list of peers to remote
     */
    public static ByteBuf encodePeer(MeshyServer master, ChannelState me) {
        try {
            ByteBuf out = ByteBufs.quickAlloc();
            ByteBufs.writeString(master.getUUID(), out);
            encodeAddress(master.getLocalAddress(), out);
            if (log.isDebugEnabled()) {
                log.debug(master + " encode peer remote=" + (me != null ? me.getChannel().remoteAddress() : ""));
            }
            for (ChannelState channelState : master.getChannelStates(MeshyConstants.LINK_NAMED)) {
                ByteBufs.writeString(channelState.getName(), out);
                encodeAddress(channelState.getRemoteAddress(), out);
                if (log.isDebugEnabled()) {
                    log.debug(master + " encoded " + channelState.getName() + " @ " + channelState.getChannel().remoteAddress());
                }
            }
            for (MeshyServer member : master.getMembers()) {
                if (member != master && shouldEncode(member.getLocalAddress())) {
                    if (log.isTraceEnabled()) {
                        log.trace("encode MEMBER: " + member.getUUID() + " / " + member.getLocalAddress());
                    }
                    ByteBufs.writeString(member.getUUID(), out);
                    encodeAddress(member.getLocalAddress(), out);
                }
            }
            ByteBufs.writeString("", out);
            return out;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * receive peer's uuid:port and peer list then connect to ones that are new to us
     */
    public static void decodePeer(MeshyServer master, ChannelState me, ByteBuf in) {
        try {
            me.setName(ByteBufs.readString(in));
            InetSocketAddress sockAddr = decodeAddress(in);
            InetAddress inAddr = sockAddr.getAddress();
            /* if remote reports loopback or any-net, use peer ip addr + port */
            if (inAddr.isAnyLocalAddress() || inAddr.isLoopbackAddress()) {
                sockAddr = new InetSocketAddress(me.getChannelRemoteAddress().getAddress(), sockAddr.getPort());
            }
            me.setRemoteAddress(sockAddr);
            if (log.isDebugEnabled()) {
                log.debug(master + " decode peer " + me.getChannel().remoteAddress() + ":" + me.getName() + ":" + me.getRemoteAddress());
            }
            while (true) {
                String peerUuid = ByteBufs.readString(in);
                if (peerUuid.isEmpty()) {
                    break;
                }
                InetSocketAddress peerAddress = decodeAddress(in);
                if (log.isDebugEnabled()) {
                    log.debug(master + " decoded " + peerUuid + " @ " + peerAddress);
                }
                peerQueue.put(new PeerTuple(master, peerUuid, peerAddress));
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
