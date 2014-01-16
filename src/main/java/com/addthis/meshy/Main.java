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
import java.io.InputStream;

import java.net.InetSocketAddress;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import java.text.DecimalFormat;

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Strings;

import com.addthis.meshy.service.file.DupFilter;
import com.addthis.meshy.service.file.FileReference;
import com.addthis.meshy.service.file.FileSource;
import com.addthis.meshy.service.host.HostNode;
import com.addthis.meshy.service.host.HostSource;
import com.addthis.meshy.service.stream.StreamSource;

public final class Main {

    public static void main(String args[]) throws Exception {
        if (args.length == 0) {
            System.out.println("usage: client <host> <port> [cmd] [arg] ... | server <port,port> [root_dir] [peer,peer]");
            return;
        }
        if (args[0].equals("client") && args.length > 2) {
            try (Meshy more = new MeshyClient(args[1], Integer.parseInt(args[2]))) {
                if (args.length > 3) {
                    String cmd = args[3];
                    if (cmd.equals("ls")) {
	                    String expr = args.length > 4 ? args[4] : "/*";
	                    if (expr.endsWith("/")) {
		                    expr += "*";
	                    }
                        FileSource fileSource = new FileSource(more, new String[]{expr});
                        fileSource.waitComplete();
                        for (FileReference file : fileSource.getFileList()) {
                            System.out.println(file.getHostUUID() + " " + file.name + " \t " + file.size + " \t " + new Date(file.lastModified));
                        }
                    } else if (cmd.equals("cat") && args.length > 5) {
                        StreamSource source = null;
                        String uuid = args[4];
                        String file = args[5];
                        if (args.length > 6) {
                            int buffer = 0;
                            Map<String, String> params = null;
                            params = new HashMap<>();
                            for (int i = 6; i < args.length; i++) {
                                String kv[] = Strings.splitArray(args[i], "=");
                                if (kv[0].equals("--buffer")) {
                                    buffer = Integer.parseInt(kv[1]);
                                } else {
                                    params.put(kv[0], kv[1]);
                                }
                            }
                            source = new StreamSource(more, uuid, file, params, buffer);
                        } else {
                            source = new StreamSource(more, uuid, file, 0);
                        }
                        try (InputStream in = source.getInputStream()) {
                            byte[] buffer = new byte[4096];
                            int read = 0;
                            while ((read = in.read(buffer)) >= 0) {
                                if (read == 0) {
                                    continue;
                                }
                                if (read < 0) {
                                    break;
                                }
                                System.out.write(buffer, 0, read);
                            }
                        }
                        source.waitComplete();
                    } else if (cmd.equals("peer")) {
                        HostSource hostSource = new HostSource(more);
                        for (int i = 4; i < args.length; i++) {
                            hostSource.addPeer(args[i]);
                        }
                        hostSource.sendRequest();
                        hostSource.waitComplete();
                        for (HostNode node : hostSource.getHostList()) {
                            System.out.println(node.uuid + " \t " + node.address);
                        }
                    } else if (cmd.equals("madcat") && args.length > 5) {
                    /* usage: madcat <readers> <bufferSize> <filematch> */
                        int threads = Integer.parseInt(args[4]);
                        final int bufferSize = Integer.parseInt(args[5]);
                        final String fileMatch[] = {args[6]};
                        final DecimalFormat number = new DecimalFormat("#,###");
                        final AtomicLong totalBytes = new AtomicLong(0);
                        final AtomicLong readBytes = new AtomicLong(0);
                        final AtomicLong lastEmit = new AtomicLong(JitterClock.globalTime());
                        final FileSource fs = new FileSource(more, fileMatch, new DupFilter());
                        fs.waitComplete();
                        final Iterator<FileReference> fsIter = fs.getFileList().iterator();
                        final HashMap<FileReference, Long> perfData = new HashMap<>();
                        final AtomicInteger open = new AtomicInteger(0);

                        class SourceReader extends Thread {

                            private FileReference current;
                            private StreamSource source;
                            private InputStream in;
                            private long start;

                            SourceReader(FileReference ref) throws IOException {
                                setup(ref);
                            }

                            private void setup(FileReference ref) throws IOException {
                                if (in != null) {
                                    perfData.put(current, System.currentTimeMillis() - start);
                                    source.waitComplete();
                                    in.close();
                                }
                                if (ref != null) {
                                    start = System.currentTimeMillis();
                                    source = new StreamSource(more, ref.getHostUUID(), ref.name, bufferSize);
                                    in = source.getInputStream();
                                    current = ref;
                                }
                            }

                            public void run() {
                                open.incrementAndGet();
                                try {
                                    read();
                                } catch (Exception ex) {
                                    ex.printStackTrace();
                                }
                                open.decrementAndGet();
                            }

                            private void read() throws Exception {
                                byte[] buffer = new byte[4096];
                                int read = 0;
                                while (true) {
                                    read = in.read(buffer);
                                    if (read == 0) {
                                        continue;
                                    }
                                    if (read < 0) {

                                        synchronized (fsIter) {
                                            if (fsIter.hasNext()) {
                                                setup(fsIter.next());
                                                continue;
                                            }
                                        }
                                        break;
                                    }
                                    totalBytes.addAndGet(read);
                                    synchronized (fs) {
                                        long sum = readBytes.addAndGet(read);
                                        long mark = JitterClock.globalTime();
                                        long time = mark - lastEmit.get();
                                        if (time > 1000) {
                                            lastEmit.set(mark);
                                            readBytes.set(0);
                                            System.out.println("rate " + number.format((sum / time) * 1000) + " open " + open.get());
                                        }
                                    }
                                }
                                setup(null);
                            }
                        }

                        List<SourceReader> readers = new LinkedList<>();
                        while (fsIter.hasNext() && threads-- > 0) {
                            readers.add(new SourceReader(fsIter.next()));
                        }
                        System.out.println("reading " + readers.size() + " sources");
                        long mark = JitterClock.globalTime();
                        for (Thread t : readers) {
                            t.start();
                        }
                        for (Thread t : readers) {
                            t.join();
                        }
                        mark = JitterClock.globalTime() - mark + 1;
                        for (Map.Entry<FileReference, Long> e : perfData.entrySet()) {
                            System.out.println(">> " + e.getKey() + " read in " + e.getValue() + " ms");
                        }
                        System.out.println("read " + number.format(totalBytes) + " in " + mark + "ms = " + number.format((totalBytes.get() / mark) * 1000) + " bytes/sec");
                    }
                }

            }
        } else if (args[0].equals("server") && args.length >= 2) {
            /**
             * server [port,port]
             * server [port,port] [rootDir]
             * server [port,port] [rootDir] [peerTo,peerTo]
             */
            String ports[] = Strings.splitArray(args[1], ",");
            LinkedList<MeshyServer> meshNodes = new LinkedList<>();
            MeshyServerGroup group = new MeshyServerGroup();
            for (String port : ports) {
                String netIf[] = null;
                String portInfo[] = Strings.splitArray(port, ":");
                int portNum = Integer.parseInt(portInfo[0]);
                if (portInfo.length > 1) {
                    netIf = new String[portInfo.length - 1];
                    System.arraycopy(portInfo, 1, netIf, 0, netIf.length);
                }
                switch (args.length) {
                    case 2:
                        meshNodes.add(new MeshyServer(portNum, new File("."), netIf, group));
                        break;
                    case 3:
                    case 4:
                        meshNodes.add(new MeshyServer(portNum, new File(args[2]), netIf, group));
                        break;
                }
            }
            if (args.length == 4) {
                for (String peer : Strings.splitArray(args[3], ",")) {
                    for (MeshyServer meshNode : meshNodes) {
                        String hostPort[] = Strings.splitArray(peer, ":");
                        int port = hostPort.length > 1 ? Integer.parseInt(hostPort[1]) : meshNode.getLocalAddress().getPort();
                        meshNode.connectPeer(new InetSocketAddress(hostPort[0], port));
                    }
                }
            }
        }
    }
}
