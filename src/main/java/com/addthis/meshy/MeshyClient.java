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

import java.net.InetSocketAddress;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.file.FileSource;
import com.addthis.meshy.service.stream.SourceInputStream;
import com.addthis.meshy.service.stream.StreamSource;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MeshyClient extends Meshy {

    private static final Logger log = LoggerFactory.getLogger(MeshyClient.class);

    /**
     * client
     */
    public MeshyClient(String host, int port) throws IOException {
        this(new InetSocketAddress(host, port));
    }

    /**
     * client
     */
    public MeshyClient(InetSocketAddress address) throws IOException {
        super();
        /* block session creation until connection is fully established */
        try {
            clientInitGate.acquire();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        ChannelFuture clientConnect = connect(address);
        clientConnect.awaitUninterruptibly();
        if (!clientConnect.isSuccess()) {
            close();
            throw new IOException("connection fail to " + address);
        }
        clientChannelCloseFuture = clientConnect.getChannel().getCloseFuture();
        /* re-acquire after connection comes up, which releases the lock */
        try {
            clientInitGate.acquire();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        if (log.isDebugEnabled()) {
            log.debug("client [" + getUUID() + "] connected to " + address);
        }
    }

    @Override
    public String toString() {
        return "MC:{" + getUUID() + ",all=" + getChannelCount() + ",sm=" + getPeeredCount() + "}";
    }

    /**
     * returns a future that notifies of channel closure
     */
    public ChannelFuture getClientChannelCloseFuture() {
        return clientChannelCloseFuture;
    }

    private final ChannelFuture clientChannelCloseFuture;
    private final Semaphore clientInitGate = new Semaphore(1);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private ChannelState clientState;
    private int bufferSize;

    /**
     * @return peer uuid <b>only</b> if this is a pure client
     *         otherwise returns a null
     */
    public String getPeerUUID() {
        return clientState != null ? clientState.getName() : null;
    }

    @Override
    protected void connectChannel(Channel channel, ChannelState channelState) {
        super.connectChannel(channel, channelState);
        clientState = channelState;
        clientInitGate.release();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            super.close();
            if (clientChannelCloseFuture != null) {
                clientChannelCloseFuture.getChannel().close().awaitUninterruptibly();
            }
        }
    }

    public MeshyClient setBufferSize(int size) {
        bufferSize = size;
        return this;
    }

    /**
     * sync version
     */
    public Collection<FileReference> listFiles(final String[] paths) throws IOException {
        if (closed.get()) {
            throw new IOException("client connection closed");
        }
        FileSource fileSource = new FileSource(this, paths);
        fileSource.waitComplete();
        return fileSource.getFileList();
    }

    /** async version */
    public void listFiles(final String[] paths, final ListCallback callback) throws IOException {
        if (closed.get()) {
            throw new IOException("client connection closed");
        }
        FileSource fileSource = new FileSource(this) {
            @Override
            public void receiveReference(FileReference ref) {
                callback.receiveReference(ref);
            }

            @Override
            public void receiveComplete() {
                callback.receiveReferenceComplete();
            }
        };
        fileSource.requestRemoteFiles(paths);
    }

    public SourceInputStream readFile(FileReference ref) throws IOException {
        return readFile(ref.getHostUUID(), ref.name);
    }

    public SourceInputStream readFile(FileReference ref, Map<String, String> options) throws IOException {
        return readFile(ref.getHostUUID(), ref.name, options);
    }

    public SourceInputStream readFile(String nodeUuid, String fileName) throws IOException {
        if (closed.get()) {
            throw new IOException("client connection closed");
        }
        return new StreamSource(this, nodeUuid, fileName, bufferSize).getInputStream();
    }

    public SourceInputStream readFile(String nodeUuid, String fileName, Map<String, String> options) throws IOException {
        if (closed.get()) {
            throw new IOException("client connection closed");
        }
        return new StreamSource(this, nodeUuid, fileName, options, bufferSize).getInputStream();
    }

    public StreamSource getFileSource(String nodeUuid, String fileName, Map<String, String> options)
            throws IOException {
        if (closed.get()) {
            throw new IOException("client connection closed");
        }
        return new StreamSource(this, nodeUuid, fileName, options, bufferSize);
    }

    /** */
    public static interface ListCallback {

        /**
         * Called each time a new file reference is received
         *
         * @param ref - the file referecne received
         */
        public void receiveReference(FileReference ref);

        /**
         * Called when all reference have been completed.  This can
         * be used to determine when the communication interaction to
         * the mesh source has completed, allowing clients to fail
         * quickly when no references are found.
         */
        public void receiveReferenceComplete();
    }

}
