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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import com.addthis.basis.util.Bytes;

import com.addthis.meshy.ChannelMaster;
import com.addthis.meshy.Meshy;
import com.addthis.meshy.SourceHandler;

import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public void receive(int length, ChannelBuffer buffer) {
        InputStream in = Meshy.getInput(length, buffer);
        String topic = null;
        try {
            topic = Bytes.readString(in);
            listener.receiveMessage(topic, in);
        } catch (Exception ex) {
            log.warn("fail to receive to topic=" + topic + " listener=" + listener + " in=" + in + " len-" + length + " buf=" + buffer + " ex=" + ex);
        }
    }

    @Override
    public void receiveComplete() throws Exception {
        listener.linkDown();
    }

    @Override
    public OutputStream sendMessage(String topic) {
        try {
            ByteArrayOutputStream out = new SendOnCloseOutputStream(this, 4096);
            Bytes.writeString(topic, out);
            return out;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
