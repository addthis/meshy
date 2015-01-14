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

/** for cmd-line stats output */
public class ServerStats {

    final int bin;
    final int bout;
    final int peerCount;
    final int channelCount;

    ServerStats(MeshyServer server) {
        bin = server.getAndClearRecv();
        bout = server.getAndClearSent();
        channelCount = server.getChannelCount();
        peerCount = server.getServerPeerCount();
        MeshyServer.peerCountMetric.clear();
        MeshyServer.peerCountMetric.inc(server.getChannelCount());
    }
}
