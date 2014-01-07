package sun.net.www.protocol.meshy;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

/**
 * Created on 1/7/14.
 */
public class Handler extends URLStreamHandler {
    @Override
    protected URLConnection openConnection(URL u) throws IOException
    {
        return new MeshyConnection(u);
    }
}