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
package com.addthis.meshy.service.message;

import com.addthis.meshy.ChannelMaster;
import com.addthis.meshy.ChannelState;
import com.addthis.meshy.SourceHandler;
import com.addthis.meshy.util.ByteBufs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

/**
 * runs in context of mesh client
 */
public class MessageSource extends SourceHandler implements OutputSender, TopicSender {

    private static final Logger log = LoggerFactory.getLogger(MessageSource.class);

    private final TopicListener listener;

    public MessageSource(ChannelMaster master, TopicListener listener) {
        super(master, MessageTarget.class);
        this.listener = listener;
    }

    @Override
    public void receive(ChannelState state, ByteBuf in) {
        String topic = null;
        try {
            topic = ByteBufs.readString(in);
            listener.receiveMessage(topic, in);
        } catch (Exception ex) {
            log.warn("fail to receive to topic={} listener={} in={} len-{}",
                    topic, listener, in, in.readableBytes(), ex);
        }
    }

    @Override
    public void channelClosed(ChannelState state) {
    }

    @Override
    public void receiveComplete() throws Exception {
        listener.linkDown();
    }

    @Override
    public SendOnCloseByteBufHolder sendMessage(String topic) {
        try {
            SendOnCloseByteBufHolder outHolder = new SendOnCloseByteBufHolder(this, 4096);
            ByteBufs.writeString(topic, outHolder.content());
            return outHolder;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
