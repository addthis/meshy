package sun.net.www.protocol.meshy;

import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.service.file.FileReference;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created on 1/7/14.
 */
public class MeshyConnection extends URLConnection {
    public MeshyConnection(URL url) {
        super(url);
    }

    private MeshyClient client;

    @Override
    public void connect() throws IOException {
        if (client != null) {
            this.client = new MeshyClient(url.getHost(), url.getPort());
        }
    }

    @Override
    public Object getContent() throws IOException {
        throw new IOException("getContent() not supported");
    }

    @Override
    public InputStream getInputStream() throws IOException {
        final AtomicReference<InputStream> in = new AtomicReference<InputStream>();
        final Semaphore lock = new Semaphore(1);
        try { lock.acquire(); } catch (Exception ex) { throw new IOException("lock exception"); }
        client.listFiles(new String[] { url.getFile() }, new MeshyClient.ListCallback() {

            @Override
            public void receiveReference(FileReference ref) {
                try {
                    in.set(client.readFile(ref));
                    lock.release();
                } catch (Exception ex) {
                    ex.printStackTrace();
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

