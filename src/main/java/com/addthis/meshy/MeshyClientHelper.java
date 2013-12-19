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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public final class MeshyClientHelper {

    /**
     * static shared connections
     */
    static final Map<String, StaticClient> meshyClients = new HashMap<>();

    private MeshyClientHelper() {
    }

    /**
     * for Hydra use ... in filters, etc ... to prevent creating many clients
     */
    public static MeshyClient getSharedInstance(String host, int port) throws IOException {
        synchronized (meshyClients) {
            String key = host + ":" + port;
            StaticClient client = meshyClients.get(key);
            if (client == null) {
                client = new StaticClient(key, host, port);
                meshyClients.put(key, client);
            }
            client.incRef();
            return client;
        }
    }

    /** */
    private static class StaticClient extends MeshyClient {

        private final AtomicInteger refCount = new AtomicInteger(0);
        private final String key;

        StaticClient(String key, String host, int port) throws IOException {
            super(host, port);
            this.key = key;
        }

        StaticClient incRef() {
            refCount.incrementAndGet();
            return this;
        }

        @Override
        public void close() {
            synchronized (meshyClients) {
                if (refCount.decrementAndGet() == 0) {
                    meshyClients.remove(key);
                    super.close();
                }
            }
        }
    }
}
