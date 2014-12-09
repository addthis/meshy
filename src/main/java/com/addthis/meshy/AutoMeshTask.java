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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.zip.CRC32;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Parameter;

import com.addthis.meshy.service.peer.PeerService;

import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AutoMeshTask implements Runnable {
    protected static final Logger log = LoggerFactory.getLogger(AutoMeshTask.class);

    private static final String secret = Parameter.value("meshy.secret");

    private final MeshyServer meshyServer;
    private final MeshyServerGroup group;
    private final int timeout;
    private final int port;

    public AutoMeshTask(MeshyServer meshyServer, MeshyServerGroup group, int timeout, int port) {
        this.meshyServer = meshyServer;
        this.group = group;
        this.timeout = timeout;
        this.port = port;
    }

    private DatagramSocket newSocket() throws SocketException {
        return new DatagramSocket(port);
    }

    @Override public void run() {
        try (final DatagramSocket server = newSocket()) {
            server.setBroadcast(true);
            server.setSoTimeout(timeout);
            server.setReuseAddress(false);
            log.info("{} AutoMesh enabled server={}", meshyServer, server.getLocalAddress());
            long lastTransmit = 0;
            while (true) {
                long time = System.currentTimeMillis();
                if (time - lastTransmit > timeout) {
                    if (log.isDebugEnabled()) {
                        log.debug("{} AutoMesh.xmit {} members", meshyServer, group.getMembers().length);
                    }
                    server.send(encode());
                    lastTransmit = time;
                }
                try {
                    DatagramPacket packet = new DatagramPacket(new byte[4096], 4096);
                    server.receive(packet);
                    log.debug("{} AutoMesh.recv from: {} size={}",
                              meshyServer, packet.getAddress(), packet.getLength());
                    if (packet.getLength() > 0) {
                        for (NodeInfo info : decode(packet)) {
                            log.debug("{} AutoMesh.recv: {} : {} from {}",
                                      meshyServer, info.uuid, info.address, info.address);
                            meshyServer.connectToPeer(info.uuid, info.address);
                        }
                    }
                } catch (SocketTimeoutException sto) {
                    // expected ... ignore
                    log.debug("{} AutoMesh listen timeout", meshyServer);
                }
            }
        } catch (Exception e) {
            log.error("{} AutoMesh exit on {}", meshyServer, e, e);
        }
    }

    private DatagramPacket encode() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MeshyServer[] members = group.getMembers();
        ArrayList<MeshyServer> readyList = Lists.newArrayList(members);
        Bytes.writeInt(readyList.size(), out);
        for (MeshyServer meshy : readyList) {
            Bytes.writeString(meshy.getUUID(), out);
            PeerService.encodeAddress(meshy.getLocalAddress(), out);
        }
        if (secret != null) {
            Bytes.writeString(secret, out);
        }
        byte[] raw = out.toByteArray();
        CRC32 crc = new CRC32();
        crc.update(raw);
        out = new ByteArrayOutputStream();
        Bytes.writeBytes(raw, out);
        Bytes.writeLength(crc.getValue(), out);
        DatagramPacket p = new DatagramPacket(out.toByteArray(), out.size());
        p.setAddress(InetAddress.getByAddress(new byte[]{(byte) 255, (byte) 255, (byte) 255, (byte) 255}));
        p.setPort(port);
        return p;
    }

    private Iterable<NodeInfo> decode(DatagramPacket packet) throws IOException {
        InetAddress remote = packet.getAddress();
        byte[] packed = packet.getData();
        ByteArrayInputStream in = new ByteArrayInputStream(packed);
        byte[] raw = Bytes.readBytes(in);
        long crcValue = Bytes.readLength(in);
        CRC32 crc = new CRC32();
        crc.update(raw);
        long crcCheck = crc.getValue();
        if (crcCheck != crcValue) {
            throw new IOException("CRC mismatch " + crcValue + " != " + crcCheck);
        }
        in = new ByteArrayInputStream(raw);
        LinkedList<NodeInfo> list = new LinkedList<>();
        int meshies = Bytes.readInt(in);
        while (meshies-- > 0) {
            String remoteUuid = Bytes.readString(in);
            InetSocketAddress address = PeerService.decodeAddress(in);
            InetAddress ina = address.getAddress();
            if (ina.isAnyLocalAddress() || ina.isLoopbackAddress()) {
                address = new InetSocketAddress(remote, address.getPort());
            }
            list.add(new NodeInfo(remoteUuid, address));
        }
        if (secret != null) {
            String compare = in.available() > 0 ? Bytes.readString(in) : "";
            /* discard peer's list if secret doesn't match */
            if (!secret.equals(compare)) {
                list.clear();
            }
        }
        return list;
    }
}
