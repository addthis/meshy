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

import com.addthis.basis.util.Bytes;
import com.addthis.meshy.service.stream.SourceInputStream;
import com.addthis.meshy.service.stream.StreamSource;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import static org.junit.Assert.*;


public class TestProtocolHandler extends TestMesh {

    @Test
    public void testProtocolHandler() throws Exception {
        final MeshyServer server1 = getServer("src/test/files");
        final MeshyClient client = getClient(server1);

        InputStream in = new URL("meshy://localhost:"+server1.getLocalPort()+"/a/abc.xml").openStream();
        assertTrue(in != null);
        byte out[] = Bytes.readFully(in);
        assertEquals(4, out.length);
        assertEquals("123", Bytes.toString(out).trim());
    }
}
