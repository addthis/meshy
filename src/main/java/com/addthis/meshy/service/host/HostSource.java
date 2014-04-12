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
package com.addthis.meshy.service.host;

import java.net.InetSocketAddress;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.addthis.meshy.ChannelMaster;
import com.addthis.meshy.ChannelState;
import com.addthis.meshy.SourceHandler;
import com.addthis.meshy.service.peer.PeerService;
import com.addthis.meshy.util.ByteBufs;

import io.netty.buffer.ByteBuf;


public class HostSource extends SourceHandler {

    private final HashMap<String, InetSocketAddress> hostMap = new HashMap<>();
    private final LinkedList<HostNode> hostList = new LinkedList<>();
    private final HashSet<String> peerAdd = new HashSet<>();

    public HostSource(ChannelMaster master) {
        super(master, HostTarget.class);
    }

    public void addPeer(String host) {
        peerAdd.add(host);
    }

    public void sendRequest() {
        try {
            ByteBuf buf = ByteBufs.quickAlloc();
            buf.writeInt(peerAdd.size());
            for (String peer : peerAdd) {
                ByteBufs.writeString(peer, buf);
            }
            send(buf);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        this.sendComplete();
    }

    public List<HostNode> getHostList() {
        return hostList;
    }

    public Map<String, InetSocketAddress> getHostMap() {
        return hostMap;
    }

    @Override
    public void channelClosed(ChannelState state) {
    }

    @Override
    public void receive(ChannelState state, ByteBuf buffer) throws Exception {
        int hosts = buffer.readInt();
        while (hosts-- > 0) {
            String uuid = ByteBufs.readString(buffer);
            InetSocketAddress address = PeerService.decodeAddress(buffer);
            hostList.add(new HostNode(uuid, address));
            hostMap.put(uuid, address);
        }
    }

    @Override
    public void receiveComplete() throws Exception {
    }
}
