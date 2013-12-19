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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.net.InetSocketAddress;

import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.addthis.basis.util.Bytes;

import com.addthis.meshy.service.stream.SourceInputStream;
import com.addthis.meshy.service.stream.StreamSource;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class TestStreamService extends TestMesh {

    private final long MD5HOSTS = -1621285313438006658L;

    //@Test
    public void testPeerLocalStream() throws Exception {
        localStreamTest(false, "read sync");
    }

    private void localStreamTest(boolean async, String logPrefix) throws Exception {
        final MeshyServer server1 = getServer("src/test/files");
        final MeshyServer server2 = getServer("src/test/files/a");
        final MeshyServer server3 = getServer("src/test/files/b");
        final MeshyServer server4 = getServer("src/test/files/c");
        server1.connectPeer(new InetSocketAddress("localhost", server2.getLocalPort()));
        server1.connectPeer(new InetSocketAddress("localhost", server3.getLocalPort()));
        server1.connectPeer(new InetSocketAddress("localhost", server4.getLocalPort()));
        /** wait for network chatter to calm down */
        waitQuiescent();

        MeshyClient client = getClient(server1);

        /* simple direct test */
        StreamSource stream = new StreamSource(client, server1.getUUID(), "/a/abc.xml", 1024 * 10);
        SourceInputStream in = stream.getInputStream();
        byte data[] = async ? readAsync(in) : Bytes.readFully(in);
        assertEquals(data.length, 4);
        log.info(logPrefix + " server1:/a/abc.xml [" + data.length + "]");
        stream.waitComplete();

        /* multi-part "meshy" test */
        stream = new StreamSource(client, server1.getUUID(), "/c/hosts", 1024 * 10);
        in = stream.getInputStream();
        data = async ? readAsync(in) : Bytes.readFully(in);
        assertEquals(data.length, 593366);
        assertEquals(MD5HOSTS, md5(data));
        log.info(logPrefix + " server1:/c/hosts [" + data.length + "]");
        stream.waitComplete();

        /* remote error test */
        final String randomString = UUID.randomUUID().toString();
        stream = new StreamSource(client, server1.getUUID(), "/" + randomString, 1024 * 10);
        try {
            in = stream.getInputStream();
            data = async ? readAsync(in) : Bytes.readFully(in);
            fail(logPrefix + " should not exist");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains(randomString));
        }
        stream.waitComplete();
        log.info(logPrefix + " pass non-existent-file test");

        /* proxied file test */
        stream = new StreamSource(client, server2.getUUID(), "/abc.xml", 1024 * 10);
        in = stream.getInputStream();
        data = async ? readAsync(in) : Bytes.readFully(in);
        assertEquals(data.length, 4);
        log.info(logPrefix + " server2:/abc.xml [" + data.length + "]");
        stream.waitComplete();

        /* proxied "meshy" file test */
        stream = new StreamSource(client, server4.getUUID(), "/hosts", 1024 * 10);
        in = stream.getInputStream();
        data = async ? readAsync(in) : Bytes.readFully(in);
        assertEquals(data.length, 593366);
        assertEquals(MD5HOSTS, md5(data));
        log.info(logPrefix + " server4:/hosts [" + data.length + "]");
        stream.waitComplete();

        /* mux'd file test */
        stream = new StreamSource(client, server1.getUUID(), "/mux/hosts", 1024 * 10);
        in = stream.getInputStream();
        data = async ? readAsync(in) : Bytes.readFully(in);
        assertEquals(data.length, 593366);
        assertEquals(MD5HOSTS, md5(data));
        log.info(logPrefix + " server1:/mux/hosts [" + data.length + "]");
        stream.waitComplete();
    }

    //@Test
    public void testPeerLocalStreamAsync() throws Exception {
        localStreamTest(true, "read async");
    }

    private byte[] readAsync(SourceInputStream in) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        boolean done = false;
        while (!done) {
            byte[] data = in.poll(100, TimeUnit.MILLISECONDS);
            if (data != null) {
                if (data.length == 0) {
                    done = true;
                } else {
                    bos.write(data);
                }
            }

        }
        return bos.toByteArray();
    }


    @Test
    public void testReadPressure() throws Exception {
        final int serverPort = nextPort.incrementAndGet();
        final int serverCount = 20;
        LinkedList<MeshyServer> servers = new LinkedList<MeshyServer>();
        for (int i = 0; i < serverCount; i++) {
            servers.add(getServer(nextPort.getAndIncrement(), "src/test/files"));
        }
        Meshy first = servers.getFirst();
        for (MeshyServer server : servers) {
            server.connectToPeer(first.getUUID(), new InetSocketAddress("localhost", serverPort));
        }
        // allow server connections to establish
        waitQuiescent();
        /* read the same file from each local server */
        log.info("-- local read --");
        long time = System.nanoTime();
        for (int i = 0; i < serverCount; i++) {
            new StreamReader(servers.get(i), servers.get(i), "c/hosts").kick().join();
        }
        long mark = System.nanoTime();
        log.info("... done in " + num.format(mark - time) + "ns ...");
        /* read the same file proxied through another server */
        log.info("-- remote read --");
        time = System.nanoTime();
        for (int i = 0; i < serverCount; i++) {
            new StreamReader(servers.get(i), servers.get((i + 1) % servers.size()), "c/hosts").kick().join();
        }
        mark = System.nanoTime();
        log.info("... done in " + num.format(mark - time) + "ns ...");
        /* concurrently read the same file proxied through another server */
        log.info("-- remote concurrent read --");
        time = System.nanoTime();
        LinkedList<Thread> threads = new LinkedList<Thread>();
        for (int i = 0; i < serverCount; i++) {
            threads.add(new StreamReader(servers.get(i), servers.get((i + 1) % servers.size()), "c/hosts").kick());
        }
        for (Thread thread : threads) {
            thread.join();
        }
        mark = System.nanoTime();
        log.info("... done in " + num.format(mark - time) + "ns ...");
        /* concurrently read the same multiplexed file proxied through another server */
        log.info("-- remote concurrent multiplexed read --");
        time = System.nanoTime();
        threads = new LinkedList<Thread>();
        for (int i = 0; i < serverCount; i++) {
            threads.add(new StreamReader(servers.get(i), servers.get((i + 1) % servers.size()), "mux/hosts").kick());
        }
        for (Thread thread : threads) {
            thread.join();
        }
        mark = System.nanoTime();
        log.info("... done in " + num.format(mark - time) + "ns ...");
    }

    private static long md5(final byte data[]) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.digest(data);
            long val = 0;
            for (byte b : md.digest()) {
                val <<= 8;
                val |= (b & 0xff);
            }
            return val;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * reader helper
     */
    private static class StreamReader extends Thread {

        private final MeshyServer connectTo;
        private final MeshyServer readFrom;
        private final String path;
        private final boolean async;

        StreamReader(final MeshyServer connectTo, final MeshyServer readFrom, String path, boolean async) throws InterruptedException {
            this.connectTo = connectTo;
            this.readFrom = readFrom;
            this.path = path;
            setName("StreamReader " + connectTo.getLocalPort() + "-" + readFrom.getLocalPort() + " @ " + path);
            this.async = async;
        }

        StreamReader(final MeshyServer connectTo, final MeshyServer readFrom, String path) throws InterruptedException {
            this(connectTo, readFrom, path, false);
        }

        public StreamReader kick() {
            start();
            return this;
        }

        public void run() {
            Meshy client = null;
            try {
                client = new MeshyClient("localhost", connectTo.getLocalAddress().getPort());
                StreamSource stream = new StreamSource(client, readFrom.getUUID(), path, 0);
                long time = System.nanoTime();
                byte raw[];
                if (async) {
                    SourceInputStream sourceInputStream = stream.getInputStream();
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    boolean done = false;
                    while (!done) {
                        byte[] data = sourceInputStream.poll(100, TimeUnit.MILLISECONDS);
                        if (data != null) {
                            if (data.length == 0) {
                                done = true;
                            } else {
                                bos.write(data);
                            }
                        }

                    }
                    raw = bos.toByteArray();
                } else {
                    raw = Bytes.readFully(stream.getInputStream());
                }
                CRC32 crc = new CRC32();
                crc.update(raw);
                log.info("[" + connectTo.getLocalAddress().getPort() + "] read [" + readFrom.getLocalAddress().getPort() + "] = [" + raw.length + ":" + crc.getValue() + "] in (" + num.format(System.nanoTime() - time) + "ns)");
                assertEquals(593366, raw.length);
                assertEquals(4164197206L, crc.getValue());
                stream.waitComplete();
            } catch (Exception ex) {
                log.warn("FAIL " + connectTo + " -- " + readFrom, ex);
            } finally {
                if (client != null) {
                    client.close();
                }
            }
        }
    }
}
