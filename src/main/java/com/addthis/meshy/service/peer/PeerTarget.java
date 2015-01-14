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
package com.addthis.meshy.service.peer;

import com.addthis.meshy.Meshy;
import com.addthis.meshy.TargetHandler;

import io.netty.buffer.ByteBuf;

import static com.addthis.meshy.service.peer.PeerService.decodeExtraPeers;
import static com.addthis.meshy.service.peer.PeerService.decodePrimaryPeer;

public class PeerTarget extends TargetHandler {

    private boolean shouldPeer = false;
    private boolean receivedStateUuid = false;

    @Override
    public void receive(int length, ByteBuf buffer) throws Exception {
        log.debug("{} decode from {}", this, getChannelState().getChannelRemoteAddress());
        if (!receivedStateUuid) {
            shouldPeer = decodePrimaryPeer(getChannelMaster(), getChannelState(), Meshy.getInput(length, buffer));
            if (shouldPeer) {
                log.debug("{} encode to {}", this, getChannelState().getChannelRemoteAddress());
                send(PeerService.encodeSelf(getChannelMaster()));
                send(PeerService.encodeExtraPeers(getChannelMaster()));
            } else {
                log.debug("writing peer cancel from {}", this);
                send(new byte[] {0}); // send byte array with a single "0" byte for the empty string
            }
            receivedStateUuid = true;
        } else {
            decodeExtraPeers(getChannelMaster(), Meshy.getInput(length, buffer));
        }
    }

    @Override
    public void channelClosed() {
    }

    @Override
    public void receiveComplete() throws Exception {
        sendComplete();
        if (!shouldPeer) {
            getChannelState().getChannel().close();
        }
    }
}
