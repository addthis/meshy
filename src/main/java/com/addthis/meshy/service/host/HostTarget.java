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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import java.net.InetSocketAddress;

import java.util.Collection;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Strings;

import com.addthis.meshy.ChannelState;
import com.addthis.meshy.Meshy;
import com.addthis.meshy.MeshyConstants;
import com.addthis.meshy.TargetHandler;
import com.addthis.meshy.service.peer.PeerService;

import io.netty.buffer.ByteBuf;

public class HostTarget extends TargetHandler {

    public void receive(int length, ByteBuf buffer) throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(Meshy.getBytes(length, buffer));
        int count = Bytes.readInt(in);
        while (count-- > 0) {
            String peer[] = Strings.splitArray(Bytes.readString(in), ":");
            String host = peer[0];
            int port = Integer.parseInt(peer[1]);
            getChannelMaster().connectToPeer(null, new InetSocketAddress(host, port));
        }
    }

    @Override
    public void receiveComplete() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Collection<ChannelState> links = getChannelMaster().getChannels(MeshyConstants.LINK_ALL);
        Bytes.writeInt(links.size(), out);
        for (ChannelState linkState : links) {
            InetSocketAddress remote = linkState.getRemoteAddress();
            if (remote == null) {
                remote = (InetSocketAddress) linkState.getChannel().remoteAddress();
                if (log.isDebugEnabled()) {
                    log.debug("missing remote for " + remote + " @ " + linkState);
                }
            }
            Bytes.writeString(linkState.getName() != null ? linkState.getName() : "<null>", out);
            PeerService.encodeAddress(remote, out);
        }
        send(out.toByteArray());
        sendComplete();
    }
}
