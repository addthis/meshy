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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.HashMap;

import com.addthis.basis.util.Bytes;

import com.addthis.meshy.MeshyClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MessageFileProvider implements TopicListener {

    private static final Logger log = LoggerFactory.getLogger(MessageFileProvider.class);

    private final TopicSender source;
    private final HashMap<String, MessageListener> listeners = new HashMap<>();

    public MessageFileProvider(MeshyClient client) {
        this.source = new MessageSource(client, this);
    }

    public void setListener(String fileName, MessageListener listener) {
        if (listener == null) {
            deleteListener(fileName);
            return;
        }
        synchronized (listeners) {
            listeners.put(fileName, listener);
            OutputStream out = source.sendMessage(MessageFileSystem.MFS_ADD);
            try {
                Bytes.writeString(fileName, out);
                out.close();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public void deleteListener(String fileName) {
        synchronized (listeners) {
            OutputStream out = source.sendMessage(MessageFileSystem.MFS_DEL);
            try {
                Bytes.writeString(fileName, out);
                out.close();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            listeners.remove(fileName);
        }
    }

    @Override
    public void receiveMessage(String fileName, InputStream in) throws IOException {
        MessageListener listener = null;
        synchronized (listeners) {
            listener = listeners.get(fileName);
        }
        if (listener != null) {
            String topic = Bytes.readString(in);
            HashMap<String, String> options = null;
            int count = Bytes.readInt(in);
            if (count > 0) {
                options = new HashMap<>(count);
                while (count > 0) {
                    count--;
                    options.put(Bytes.readString(in), Bytes.readString(in));
                }
            }
            listener.requestContents(fileName, options, source.sendMessage(topic));
        } else {
            log.info("receive for topic with no listener: {}", fileName);
        }
    }

    @Override
    public void linkDown() {
        // subclass and override to hide this message
        log.info("link down source={} listeners={}", source, listeners);
    }
}
