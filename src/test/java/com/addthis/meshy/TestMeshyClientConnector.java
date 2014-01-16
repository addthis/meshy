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

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestMeshyClientConnector extends TestMesh {

    @Test
    public void simple() throws Exception {
        final AtomicReference<MeshyServer> server = new AtomicReference<>();
        final LinkedList<String> sequence = new LinkedList<>();
        final LinkedList<String> observed = new LinkedList<>();
        server.set(getServer());
        sequence.add("up-" + server.get().getUUID());
        MeshyClientConnector connector = new MeshyClientConnector("localhost", server.get().getLocalPort(), 500, 1000) {
            @Override
            public void linkUp(MeshyClient client) {
                observed.add("up-" + server.get().getUUID());
            }

            @Override
            public void linkDown(MeshyClient client) {
                observed.add("down-" + server.get().getUUID());
            }
        };
        try {
        Thread.sleep(1000);
        server.get().close();
        sequence.add("down-" + server.get().getUUID());
        server.set(getServer(server.get().getLocalPort()));
        sequence.add("up-" + server.get().getUUID());
        Thread.sleep(2000);
        server.get().close();
        sequence.add("down-" + server.get().getUUID());
        Thread.sleep(1000);
        log.info("seq:{}", sequence);
        log.info("obs:{}", observed);
        assertEquals(sequence.toString(), observed.toString());
        } finally {
            connector.terminate();
        }
    }
}
