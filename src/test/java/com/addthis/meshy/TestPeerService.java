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

import java.net.InetSocketAddress;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.addthis.meshy.service.host.HostSource;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestPeerService extends TestMesh {
    private static final Logger log = LoggerFactory.getLogger(TestPeerService.class);

    @Test
    public void twoPeers() throws Exception {
        MeshyServer server1 = getServer();
        MeshyServer server2 = getServer();
        log.debug("server1: {}, server2: {}", server1, server2);
        server1.connectToPeer(server2.getUUID(), server2.getLocalAddress());
        waitQuiescent();
        assertEquals(1, server1.getServerPeerCount());
        assertEquals(1, server2.getServerPeerCount());
    }

    @Test
    public void threePeersWithDisconnect() throws Exception {
        final MeshyServer server1 = getServer();
        final MeshyServer server2 = getServer();
        final MeshyServer server3 = getServer();
        server1.connectPeer(new InetSocketAddress("localhost", server2.getLocalPort()));
        server1.connectPeer(new InetSocketAddress("localhost", server2.getLocalPort()));
        server1.connectPeer(new InetSocketAddress("localhost", server2.getLocalPort()));
        server1.connectPeer(new InetSocketAddress("localhost", server3.getLocalPort()));
        // allow server connections to establish
        waitQuiescent();
        assertEquals(2, server1.getServerPeerCount());
        assertEquals(2, server2.getServerPeerCount());
        assertEquals(2, server3.getServerPeerCount());
        Meshy client = getClient(server1);
        HostSource hosts = new HostSource(client);
        hosts.sendRequest();
        hosts.waitComplete();
        log.info("host list.1 --> {}", hosts.getHostList());
        Map<String, InetSocketAddress> hostMap = hosts.getHostMap();
        assertTrue(hostMap.containsKey(server2.getUUID()));
        assertTrue(hostMap.containsKey(server3.getUUID()));
        assertEquals(hostMap.get(server2.getUUID()).getPort(), server2.getLocalPort());
        assertEquals(hostMap.get(server3.getUUID()).getPort(), server3.getLocalPort());
        client.close();

        // have one server drop out
        server2.close();
        // allow server connections to stabilize
        waitQuiescent();
        assertEquals(1, server1.getServerPeerCount());
        assertEquals(1, server3.getServerPeerCount());

        client = getClient(server1);
        hosts = new HostSource(client);
        hosts.sendRequest();
        hosts.waitComplete();
        log.info("host list.2 --> {}", hosts.getHostList());
        hostMap = hosts.getHostMap();
        assertTrue(!hostMap.containsKey(server2.getUUID()));
        assertTrue(hostMap.containsKey(server3.getUUID()));
        assertEquals(hostMap.get(server3.getUUID()).getPort(), server3.getLocalPort());
    }

    @Test
    public void manyPeers() throws Exception {
        final int serverCount = 20;
        List<MeshyServer> servers = new LinkedList<>();
        for (int i = 0; i < serverCount; i++) {
            servers.add(getServer());
        }
        MeshyServer first = servers.get(0);
        for (MeshyServer server : servers) {
            server.connectToPeer(first.getUUID(), first.getLocalAddress());
        }

        // allow server connections to establish
        log.info("waiting for servers to become idle");
        log.info("wait successful = {}", waitQuiescent());

        for (MeshyServer server : servers) {
            log.info("check connection count >> {}", server);
            try (Meshy client = getClient(server.getLocalPort())) {
                HostSource hosts = new HostSource(client);
                hosts.sendRequest();
                hosts.waitComplete();
                Map<String, InetSocketAddress> hostMap = hosts.getHostMap();
                for (MeshyServer peer : servers) {
                    if (server == peer) {
                        continue;
                    }
                    assertTrue(server + " is missing peer --> " + peer, hostMap.containsKey(peer.getUUID()));
                }
            }
        }
    }
}
