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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.addthis.basis.util.Bytes;

import com.addthis.meshy.Meshy;
import com.addthis.meshy.TargetHandler;

import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MessageService allows transient client connections to offer services and extensions
 * to long-running core mesh services.  Query is an example of this, but it's not
 * currently done that way.  RPC and virtual file extensions are likely implementations.
 */
public class MessageTarget extends TargetHandler implements OutputSender, TopicSender {

    private static final Logger log = LoggerFactory.getLogger(MessageTarget.class);

    private static final HashMap<String, TargetListener[]> targetListeners = new HashMap<>();

    public static void registerListener(String topic, TargetListener listener) {
        synchronized (targetListeners) {
            TargetListener[] list = targetListeners.get(topic);
            if (list == null) {
                list = new TargetListener[1];
            } else {
                TargetListener[] newlist = new TargetListener[list.length+1];
                System.arraycopy(list, 0, newlist, 0, list.length);
                list = newlist;
            }
            targetListeners.put(topic, list);
            list[list.length-1]=listener;
        }
    }

    public static void deregisterListener(String topic, TargetListener listener) {
        synchronized (targetListeners) {
            TargetListener[] list = targetListeners.get(topic);
            if (list != null) {
                for (int i=0; i<list.length; i++) {
                    if (list[i] == listener) {
                        TargetListener[] newlist = new TargetListener[list.length-1];
                        System.arraycopy(list, 0, newlist, 0, i);
                        if (i < list.length-1) {
                            System.arraycopy(list, i+1, newlist, i, newlist.length-i);
                        }
                        list = newlist;
                        return;
                    }
                }
            }
        }
    }

    public static void deregisterAllListeners(String topic) {
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
    public void receive(int length, ChannelBuffer buffer) throws Exception {
        InputStream in = Meshy.getInput(length, buffer);
        String topic = Bytes.readString(in);
        synchronized (targetListeners) {
            TargetListener[] list = targetListeners.get(topic);
            if (list != null) {
                for (TargetListener listener : list) {
                    listener.receiveMessage(this, topic, in);
                }
            }
        }
    }

    @Override
    public void receiveComplete() throws Exception {
        synchronized (targetListeners) {
            for (TargetListener[] list : targetListeners.values()) {
                for (TargetListener listener : list) {
                    listener.linkDown(this);
                }
            }
        }
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
