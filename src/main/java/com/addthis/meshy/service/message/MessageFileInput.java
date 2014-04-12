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

import java.io.IOException;

import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.util.Parameter;

import com.addthis.meshy.filesystem.VirtualFileInput;
import com.addthis.meshy.util.ByteBufs;

import io.netty.buffer.ByteBuf;

class MessageFileInput implements VirtualFileInput, TargetListener {

    private static final long RPC_TIMEOUT = Parameter.longValue("meshy.rpc.timeout", 5000);

    private final String name;
    private final Map<String, String> options;
    private final TopicSender target;
    private final AtomicBoolean isEOF = new AtomicBoolean(false);
    private final Semaphore gate = new Semaphore(1);
    private final String topicID = "rpc.reply." + MessageFileSystem.nextReplyID.incrementAndGet();
    private ByteBuf data;

    MessageFileInput(String name, Map<String, String> options, TopicSender target) {
        this.name = name;
        this.options = options;
        this.target = target;
    }

    /**
     * NOTE: this can be optimized to return null on the first call
     * or after "wait" is reached thus freeing up Sender threads. subsequent
     * calls can retrieve data is available or return null and set EOF is
     * max timeout is passed.
     * <p/>
     * in other words, yes, this is not a perfect implementation and under
     * sever load could back up senders.  again, in the constant game of right
     * vs right now, we are choosing right now.
     */
    @Override
    public Object nextMessage(long wait) {
        /* enter this method once only */
        if (!isEOF.compareAndSet(false, true)) {
            return null;
        }
        MessageTarget.registerListener(topicID, this);
        try {
            final SendOnCloseByteBufHolder out = target.sendMessage(name);
            if (out == null && target instanceof InternalHandler) {
                return ((InternalHandler) target).handleMessageRequest(name, options);
            }
            ByteBufs.writeString(topicID, out.content());
            if (options != null) {
                out.content().writeInt(options.size());
                for (Map.Entry<String, String> e : options.entrySet()) {
                    ByteBufs.writeString(e.getKey(), out.content());
                    ByteBufs.writeString(e.getValue(), out.content());
                }
            } else {
                out.content().writeByte(0);
            }
            out.close();
            gate.acquire();
            long maxWait = RPC_TIMEOUT;
            if (options != null) {
                String altMax = options.get(MessageFileSystem.READ_TIMEOUT);
                if (altMax != null) {
                    maxWait = Long.parseLong(altMax);
                }
            }
            if (gate.tryAcquire(maxWait, TimeUnit.MILLISECONDS)) {
                return data;
            }
        } catch (Exception ex) {
            MessageFileSystem.log.warn("MessageFileInput exception", ex);
        } finally {
            MessageTarget.deregisterListener(topicID);
        }
        return null;
    }

    @Override
    public boolean isEOF() {
        return isEOF.get();
    }

    @Override
    public void close() {
        // noop
    }

    @Override
    public void receiveMessage(TopicSender ignored, String topic, ByteBuf in) throws IOException {
        if (topic.equals(topicID) && data == null) {
            data = in.copy();
            gate.release();
        } else {
            MessageFileSystem.log.warn(
                    "received reply on invalid topic topic={} data={}", topic, data);
        }
    }

    @Override
    public void linkDown(TopicSender ignored) {
        // ignore?
    }
}
