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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetSocketAddress;
import java.util.List;

class AutoConnectToPeersTask implements Runnable {
    protected static final Logger log = LoggerFactory.getLogger(AutoConnectToPeersTask.class);

    private final MeshyServer meshyServer;
    private final List<InetSocketAddress> addresses;
    private final int timeout;

    public AutoConnectToPeersTask(MeshyServer meshyServer, List<InetSocketAddress> addresses, int timeout) {
        this.meshyServer = meshyServer;
        this.addresses = addresses;
        this.timeout = timeout;
    }

    @Override public void run() {
        while (true) {
            log.info("mss will try connection to seeds again in: " + timeout + " milliseconds.");
            try {
                Thread.sleep(timeout);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (InetSocketAddress address : addresses) {
                meshyServer.connectPeer(address);
            }
        }
    }

}
