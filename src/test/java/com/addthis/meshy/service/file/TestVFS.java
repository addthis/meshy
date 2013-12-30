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

import java.io.File;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import com.addthis.basis.util.Strings;

import com.addthis.meshy.LocalFileHandler;
import com.addthis.meshy.LocalFileSystem;
import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.MeshyServer;
import com.addthis.meshy.TestMesh;
import com.addthis.meshy.VirtualFileFilter;
import com.addthis.meshy.VirtualFileInput;
import com.addthis.meshy.VirtualFileReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TestVFS extends TestMesh {

    @Before
    public void setup() {
        System.setProperty("mesh.local.handlers", Strings.join(new String[]{
                TestVFS.DummyHandler.class.getName(),          // for running in IntelliJ
                TestVFS.DummyHandler.class.getCanonicalName()  // for running from cmd-line & maven
        }, ","));
        LocalFileSystem.reloadHandlers();
        MeshyServer.resetFileSystems();
    }

    @After
    public void cleanup() {
        System.setProperty("mesh.local.handlers", "");
        LocalFileSystem.reloadHandlers();
        MeshyServer.resetFileSystems();
    }

    @Test
    public void testVFS() throws Exception {
        final MeshyServer server = getServer("src/test");
        final MeshyClient client = getClient(server);
        FileSource files = new FileSource(client, new String[]{"*"});
        files.waitComplete();
        Map<String, FileReference> map = files.getFileMap();
        System.out.println("map=" + map);
        checkFile(map, new FileReference("/dummy", 0, 0).setHostUUID(server.getUUID()));
    }

    /** */
    public static class DummyHandler implements LocalFileHandler {

        LinkedList<VirtualFileReference> list = new LinkedList<VirtualFileReference>();
        VirtualFileReference ref = new DummyReference();

        {
            list.add(ref);
        }

        @Override
        public boolean canHandleDirectory(File dir) {
            return true;
        }

        @Override
        public Iterator<VirtualFileReference> listFiles(File dir, VirtualFileFilter filter) {
            return list.iterator();
        }

        @Override
        public VirtualFileReference getFile(File dir, String name) {
            return ref;
        }
    }

    static class DummyReference implements VirtualFileReference {

        @Override
        public String getName() {
            return "dummy";
        }

        @Override
        public long getLastModified() {
            return 0;
        }

        @Override
        public long getLength() {
            return 0;
        }

        @Override
        public Iterator<VirtualFileReference> listFiles(VirtualFileFilter filter) {
            return null;
        }

        @Override
        public VirtualFileReference getFile(String name) {
            return null;
        }

        @Override
        public VirtualFileInput getInput(Map<String, String> options) {
            return null;
        }
    }
}
