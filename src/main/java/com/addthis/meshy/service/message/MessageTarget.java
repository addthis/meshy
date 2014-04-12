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

import java.util.HashMap;

import com.addthis.meshy.TargetHandler;
import com.addthis.meshy.util.ByteBufs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

/**
 * MessageService allows transient client connections to offer services and extensions
 * to long-running core mesh services.  Query is an example of this, but it's not
 * currently done that way.  RPC and virtual file extensions are likely implementations.
 */
public class MessageTarget extends TargetHandler implements OutputSender, TopicSender {

    private static final Logger log = LoggerFactory.getLogger(MessageTarget.class);

    private static final HashMap<String, TargetListener> targetListeners = new HashMap<>();

    public static void registerListener(String topic, TargetListener listener) {
        synchronized (targetListeners) {
            if (targetListeners.put(topic, listener) != null) {
                log.warn("WARNING: override listener for " + topic);
            }
        }
    }

    @Deprecated
    public static void deregisterListener(String topic, TargetListener ignored) {
        deregisterListener(topic);
    }

    public static void deregisterListener(String topic) {
        synchronized (targetListeners) {
            targetListeners.remove(topic);
        }
    }

    @Override
    public void channelClosed() {
        // target listener's only api method that would make sense is link down
        // this will be called in a second anyway, so ignore
    }

    @Override
    public void receive(ByteBuf in) throws Exception {
        String topic = ByteBufs.readString(in);
        synchronized (targetListeners) {
            TargetListener listener = targetListeners.get(topic);
            if (listener != null) {
                listener.receiveMessage(this, topic, in);
            }
        }
    }

    @Override
    public void receiveComplete() throws Exception {
        synchronized (targetListeners) {
            for (TargetListener listener : targetListeners.values()) {
                listener.linkDown(this);
            }
        }
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
