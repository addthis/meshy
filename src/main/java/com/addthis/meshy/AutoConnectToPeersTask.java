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

import com.addthis.basis.util.LessBytes;
import com.addthis.basis.util.Parameter;
import com.addthis.meshy.service.peer.PeerService;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.List;
import java.util.zip.CRC32;

class AutoConnectToPeersTask implements Runnable {
    protected static final Logger log = LoggerFactory.getLogger(AutoConnectToPeersTask.class);

    private final MeshyServer meshyServer;
    private final List<InetSocketAddress> peers;
    private final int unitDelay;
    private final int maxDelayUnit;

    public AutoConnectToPeersTask(MeshyServer meshyServer, List<InetSocketAddress> addresses, int unitDelay, int maxDelayUnit) {
        this.meshyServer = meshyServer;
        this.peers = addresses;
        this.unitDelay = unitDelay;
        this.maxDelayUnit = maxDelayUnit;
    }

    @Override public void run() {
        try {
            Backoff backoff = new Backoff(unitDelay, maxDelayUnit);
            while (true) {
                log.info("mss will retry connection to seeds in: " + backoff.calcDelay() + " milliseconds.");
                backoff.backoff();
                backoff.inc();
                for (InetSocketAddress address : peers) {
                    meshyServer.connectPeer(address);
                }
            }
        } catch (Exception e) {
            log.error("{} AutoConnectToPeers exit on {}", meshyServer, e, e);
        }
    }

}
