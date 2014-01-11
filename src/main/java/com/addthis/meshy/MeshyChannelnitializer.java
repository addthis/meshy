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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

class MeshyChannelnitializer extends ChannelInitializer<SocketChannel> {

    private Meshy meshy;

    public MeshyChannelnitializer(Meshy meshy) {
        this.meshy = meshy;
    }

    @Override
    public void initChannel(SocketChannel channel) throws Exception {
        channel.pipeline().addLast(new MeshyChannelHandler(meshy));
    }

    private static ChannelHandler getMeshyFrameDecoder() {
        return new LengthFieldBasedFrameDecoder(16384, 8, 4);
    }
}
