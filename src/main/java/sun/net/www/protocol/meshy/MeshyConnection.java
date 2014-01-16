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
package sun.net.www.protocol.meshy;

import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.service.file.FileReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This could be extended to use the query portion of the URL to control other behaviors
 * like selecting source, producing directory listings, adding read fault tolerance, etc
 */
public class MeshyConnection extends URLConnection {
    private static final Logger log = LoggerFactory.getLogger(MeshyConnection.class);

    public MeshyConnection(URL url) {
        super(url);
    }

    private MeshyClient client;

    @Override
    public void connect() throws IOException {
        if (client == null) {
            this.client = new MeshyClient(url.getHost(), url.getPort());
        }
    }

    @Override
    public Object getContent() throws IOException {
        throw new IOException("getContent() not supported");
    }

    @Override
    public InputStream getInputStream() throws IOException {
        connect();
        final AtomicReference<InputStream> in = new AtomicReference<InputStream>();
        final Semaphore lock = new Semaphore(1);
        try { lock.acquire(); } catch (Exception ex) { throw new IOException("lock exception"); }
        client.listFiles(new String[] { url.getFile() }, new MeshyClient.ListCallback() {

            @Override
            public void receiveReference(FileReference ref) {
                try {
                    in.set(client.readFile(ref));
                } catch (Exception ex) {
                    log.trace("file list fail", ex);
                } finally {
                    lock.release();
                }
            }

            @Override
            public void receiveReferenceComplete() {
                if (in.get() == null) {
                    lock.release();
                }
            }
        });
        try { lock.acquire(); } catch (Exception ex) { throw new IOException("lock exception"); }
        if (in.get() != null) {
            return in.get();
        }
        throw new FileNotFoundException(url.getFile());
    }
}

