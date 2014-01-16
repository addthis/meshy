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
package com.addthis.meshy.service.message;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Map;

import com.addthis.basis.util.Bytes;

import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.file.FileSource;
import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.MeshyServer;
import com.addthis.meshy.TestMesh;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestMessageFileSystem extends TestMesh {

    @Test
    public void basicTest() throws Exception {
        final MeshyServer server = getServer("/tmp");
        final MeshyClient client = getClient(server);

        /*
         * client registers rpc endpoint in meshy filespace: /rpc.test/one.rpc
         */
        MessageFileProvider provider = new MessageFileProvider(client);
        provider.setListener("/rpc.test/one.rpc", new MessageListener() {
            @Override
            public void requestContents(String fileName, Map<String, String> options, OutputStream out) throws IOException {
                /* this is the client rpc reply endpoint implementation */
                Bytes.writeString("rpc.reply", out);
                /* bytes are accumulated and sent on close */
                out.close();
            }
        });

        /*
         * any client can then discover the rpc/file endpoint via normal FileService calls
         */
        FileSource files = new FileSource(client, new String[]{"/rpc.test/*.rpc"});
        files.waitComplete();
        Map<String, FileReference> map = files.getFileMap();

        log.info("files = {}", map);

        assertTrue(map.containsKey("/rpc.test/one.rpc"));

        /*
         * rpc is called/read as a normal file
         */
        FileReference ref = map.get("/rpc.test/one.rpc");
        InputStream in = client.readFile(ref);
        String str = Bytes.readString(in);

        assertEquals("rpc.reply", str);
    }

}
