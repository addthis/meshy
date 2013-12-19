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
import com.addthis.meshy.MeshyServer;
import com.addthis.meshy.SourceHandler;

import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;

public class PeerSource extends SourceHandler {

    static final Logger log = PeerService.log;

    public PeerSource(MeshyServer master, String tempUuid) {
        super(master, PeerTarget.class, tempUuid);
        if (log.isDebugEnabled()) {
            log.debug(this + " encode to " + getPeerString());
        }
        send(PeerService.encodePeer(master, null));
        sendComplete();
    }

    @Override
    public void receive(int length, ChannelBuffer buffer) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug(this + " decode from " + getPeerString());
        }
        PeerService.decodePeer((MeshyServer) getChannelMaster(), getChannelState(), Meshy.getInput(length, buffer));
    }

    @Override
    public void receiveComplete() throws Exception {
    }
}
