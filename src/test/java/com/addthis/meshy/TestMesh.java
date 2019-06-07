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

import java.io.File;
import java.io.IOException;

import java.util.LinkedList;
import java.util.Map;

import java.text.DecimalFormat;

import com.addthis.basis.util.JitterClock;

import com.addthis.meshy.service.file.FileReference;

import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class TestMesh {

    protected static final Logger log = LoggerFactory.getLogger(TestMesh.class);
    protected static final DecimalFormat num = new DecimalFormat("0,000");

    static {
        System.setProperty("meshy.autoMesh", "false");
        System.setProperty("meshy.peer.local", "true");
        System.setProperty("meshy.stats.time", "0");
    }

    public static void checkFile(Map<String, FileReference> map, FileReference test) {
        FileReference check = map.get(test.name);
        assertNotNull("missing " + test.name, check);
        assertEquals(check.name, test.name);
        assertEquals(check.size, test.size);
        assertEquals(check.getHostUUID(), test.getHostUUID());
    }

    private final LinkedList<Meshy> resources = new LinkedList<>();

    public MeshyClient getClient(MeshyServer server) throws IOException {
        return getClient(server.getLocalPort());
    }

    public MeshyClient getClient(int port) throws IOException {
        MeshyClient client = new MeshyClient("localhost", port);
        resources.add(client);
        return client;
    }

    public MeshyServer getServer() throws IOException {
        return getServer(".");
    }

    public MeshyServer getServer(String root) throws IOException {
        return getServer(0, root);
    }

    public MeshyServer getServer(int port) throws IOException {
        return getServer(port, ".");
    }

    public MeshyServer getServer(int port, String root) throws IOException {
        MeshyServer server = new MeshyServer(port, new File(root), null);
        resources.add(server);
        return server;
    }

    public boolean waitQuiescent() throws InterruptedException {
        return waitQuiescent(2000, 30000);
    }

    /**
     * wait for all resources to quiesce activity for a time
     *
     * @return true if resources meet quiescent criteria
     */
    public boolean waitQuiescent(long quiescentTime, long maxWait) throws InterruptedException {
        long mark = JitterClock.globalTime();
        while (true) {
            long min = quiescentTime;
            for (Meshy meshy : resources) {
                min = Math.min(min, JitterClock.globalTime() - meshy.lastEventTime());
            }
            if (min == quiescentTime) {
                return true;
            }
            if (JitterClock.globalTime() - mark > maxWait) {
                return false;
            }
            Thread.sleep(100);
        }
    }

    @After
    public void cleanup() {
        log.info("closing resources: {}", resources.size());
        for (Meshy meshy : resources) {
            try {
                meshy.closeAsync();
            } catch (Exception ex) {
                log.warn("unable to cleanup", ex);
            }
        }
        for (Meshy meshy : resources) {
            try {
                meshy.close();
            } catch (Exception ex) {
                log.warn("unable to cleanup", ex);
            }
        }
    }
}
