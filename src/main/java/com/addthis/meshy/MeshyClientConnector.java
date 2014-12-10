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

import java.io.IOException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * attempts to re-establish client connections when they fail
 */
public abstract class MeshyClientConnector extends Thread {

    private final String host;
    private final int port;
    private final long initDelay;
    private final long retryWait;
    private final AtomicBoolean done = new AtomicBoolean(false);
    private final AtomicReference<MeshyClient> ref = new AtomicReference<>(null);

    /**
     * @param host      meshy server host
     * @param port      meshy server port
     * @param initDelay delay before initial connection
     * @param retryWait delay between re-connect attempts
     */
    public MeshyClientConnector(String host, int port, long initDelay, long retryWait) {
        this.host = host;
        this.port = port;
        this.initDelay = initDelay;
        this.retryWait = retryWait;
        setDaemon(true);
        setName("MeshyClient Re-Connector to " + host + ":" + port);
        start();
    }

    public abstract void linkUp(MeshyClient client);

    public abstract void linkDown(MeshyClient client);

    public MeshyClient getClient() {
        return ref.get();
    }

    public void terminate() {
        done.set(true);
        interrupt();
    }

    @Override
    public void run() {
        if (initDelay > 0) {
            try {
                Thread.sleep(initDelay);
            } catch (Exception ignored) {
            }
        }
        while (!done.get()) {
            try {
                MeshyClient client = new MeshyClient(host, port);
                client.getClientChannelCloseFuture().addListener(future -> {
                    linkDown(ref.getAndSet(null));
                });
                ref.set(client);
                linkUp(client);
                while (ref.get() != null) {
                    Thread.sleep(500);
                }
            } catch (InterruptedException ex) {
                // expected on terminate()
            } catch (IOException ex) {
                try {
                    Thread.sleep(retryWait);
                } catch (Exception ignored) {
                }
            }
        }
    }
}
