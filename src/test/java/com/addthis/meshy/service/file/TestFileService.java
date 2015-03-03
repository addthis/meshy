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
package com.addthis.meshy.service.file;

import java.net.InetSocketAddress;

import java.util.Map;

import com.addthis.meshy.Meshy;
import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.MeshyServer;
import com.addthis.meshy.TestMesh;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;


public class TestFileService extends TestMesh {

    @Test
    public void singlePeer() throws Exception {
        final MeshyServer server = getServer("src/test/files");
        final MeshyClient client = getClient(server);
        FileSource files = new FileSource(client, new String[]{
                "*/*.xml",
                "*/hosts",
        });
        files.waitComplete();
        log.info("file.list --> {}", files.getFileList());
        Map<String, FileReference> map = files.getFileMap();
        checkFile(map, new FileReference("/a/abc.xml", 0, 4).setHostUUID(server.getUUID()));
        checkFile(map, new FileReference("/a/def.xml", 0, 7).setHostUUID(server.getUUID()));
        checkFile(map, new FileReference("/b/ghi.xml", 0, 4).setHostUUID(server.getUUID()));
        checkFile(map, new FileReference("/b/jkl.xml", 0, 7).setHostUUID(server.getUUID()));
        checkFile(map, new FileReference("/c/hosts", 0, 593366).setHostUUID(server.getUUID()));
        checkFile(map, new FileReference("/mux/hosts", 0, 593366).setHostUUID(server.getUUID()));
        /** second test exercises the cache */
        files = new FileSource(client, new String[]{
                "*/*.xml",
                "*/hosts",
        });
        files.waitComplete();
        log.info("file.list --> {}", files.getFileList());
        map = files.getFileMap();
        checkFile(map, new FileReference("/a/abc.xml", 0, 4).setHostUUID(server.getUUID()));
        checkFile(map, new FileReference("/a/def.xml", 0, 7).setHostUUID(server.getUUID()));
        checkFile(map, new FileReference("/b/ghi.xml", 0, 4).setHostUUID(server.getUUID()));
        checkFile(map, new FileReference("/b/jkl.xml", 0, 7).setHostUUID(server.getUUID()));
        checkFile(map, new FileReference("/c/hosts", 0, 593366).setHostUUID(server.getUUID()));
        checkFile(map, new FileReference("/mux/hosts", 0, 593366).setHostUUID(server.getUUID()));
    }

    @Test
    public void globSyntax() throws Exception {
        final MeshyServer server = getServer("src/test/files");
        final MeshyClient client = getClient(server);
        FileSource files = new FileSource(client, new String[]{
                "/{a,c}/*",
                "*/h?sts",
        });
        files.waitComplete();
        log.info("file.list --> {}", files.getFileList());
        Map<String, FileReference> map = files.getFileMap();
        checkFile(map, new FileReference("/a/abc.xml", 0, 4).setHostUUID(server.getUUID()));
        checkFile(map, new FileReference("/a/def.xml", 0, 7).setHostUUID(server.getUUID()));
        assertFalse(map.containsKey("/b/ghi.xml"));
        assertFalse(map.containsKey("/b/jkl.xml"));
        checkFile(map, new FileReference("/c/hosts", 0, 593366).setHostUUID(server.getUUID()));
        checkFile(map, new FileReference("/mux/hosts", 0, 593366).setHostUUID(server.getUUID()));
    }

    @Test
    public void multiPeer() throws Exception {
        final MeshyServer server1 = getServer("src/test/files/a");
        final MeshyServer server2 = getServer("src/test/files/b");
        final MeshyServer server3 = getServer("src/test/files");
        server1.connectPeer(new InetSocketAddress("localhost", server2.getLocalPort()));
        server1.connectPeer(new InetSocketAddress("localhost", server3.getLocalPort()));
        // allow server connections to establish
        waitQuiescent();
        Meshy client = getClient(server1);
        FileSource files = new FileSource(client, new String[]{"*.xml"});
        files.waitComplete();
        log.info("file.list --> {}", files.getFileList());
        Map<String, FileReference> map = files.getFileMap();
        log.info("file map --> {}", map);
        checkFile(map, new FileReference("/abc.xml", 0, 4).setHostUUID(server1.getUUID()));
        checkFile(map, new FileReference("/def.xml", 0, 7).setHostUUID(server1.getUUID()));
        checkFile(map, new FileReference("/ghi.xml", 0, 4).setHostUUID(server2.getUUID()));
        checkFile(map, new FileReference("/jkl.xml", 0, 7).setHostUUID(server2.getUUID()));
        checkFile(map, new FileReference("/xyz.xml", 0, 10).setHostUUID(server3.getUUID()));
    }

    @Test
    public void testEquals() throws Exception {
        FileReference[] refs = new FileReference[5];

        refs[0] = new FileReference("/a/abc.xml", 0, 4);
        refs[1] = new FileReference("/a/def.xml", 0, 7);
        refs[2] = new FileReference(null, 0, 4);

        // refs[0] and refs[3] are equal
        refs[3] = new FileReference("/a/abc.xml", 0, 4);

        // refs[2] and refs[4] are equal
        refs[4] = new FileReference(null, 0, 4);

        refs[0].setHostUUID("a");
        refs[1].setHostUUID("b");
        // verify that equals() handles hostUUID of null
        refs[2].setHostUUID(null);

        refs[3].setHostUUID("a");  // equal to refs[0]
        refs[4].setHostUUID(null); // equal to refs[2]

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                if (i == j) {
                    assertEquals(refs[i], refs[j]);
                } else {
                    assertNotEquals(refs[i], refs[j]);
                }
            }
        }

        assertEquals(refs[0], refs[3]);
        assertEquals(refs[2], refs[4]);

        refs[3].setHostUUID("b");
        refs[4].setHostUUID("b");

        assertFalse(refs[0].equals(refs[3]));
        assertFalse(refs[2].equals(refs[4]));
    }

    @Test
    public void testHashCode() throws Exception {
        FileReference[] refs = new FileReference[4];

        refs[0] = new FileReference("/a/abc.xml", 0, 4);
        refs[1] = new FileReference("/a/abc.xml", 0, 4);
        refs[2] = new FileReference(null, 0, 4);
        refs[3] = new FileReference(null, 0, 4);

        refs[0].setHostUUID("a");
        refs[1].setHostUUID("a");
        refs[2].setHostUUID(null);
        refs[3].setHostUUID(null);

        assertEquals(refs[0].hashCode(), refs[1].hashCode());
        assertEquals(refs[2].hashCode(), refs[3].hashCode());
    }

}
