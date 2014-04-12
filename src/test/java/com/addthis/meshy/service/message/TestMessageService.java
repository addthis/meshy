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

import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.MeshyServer;
import com.addthis.meshy.TestMesh;
import com.addthis.meshy.util.ByteBufs;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestMessageService extends TestMesh {

    @Test
    public void basic() throws Exception {
        final MeshyServer server = getServer("src/test/files");
        final MeshyClient client = getClient(server.getLocalPort());
        final AtomicBoolean clientRecv = new AtomicBoolean(false);
        final AtomicBoolean serverRecv = new AtomicBoolean(false);
        /** connect client and set up listener */
        TopicSender mss = new MessageSource(client, new TopicListener() {
            @Override
            public void receiveMessage(String topic, ByteBuf message) throws IOException {
                log.info("client recv: {}", topic);
                assertEquals("def", topic);
                assertEquals("67890", ByteBufs.readString(message));
                clientRecv.set(true);
            }

            @Override
            public void linkDown() {
                log.info("client linkdown");
            }
        });
        /** register server-side listener */
        MessageTarget.registerListener("abc", new TargetListener() {
            @Override
            public void receiveMessage(TopicSender target, String topic, ByteBuf message) throws IOException {
                log.info("server recv: {}", topic);
                assertEquals("abc", topic);
                assertEquals("12345", ByteBufs.readString(message));
                SendOnCloseByteBufHolder out = target.sendMessage("def");
                ByteBufs.writeString("67890", out.content());
                out.close();
                serverRecv.set(true);
            }

            @Override
            public void linkDown(TopicSender target) {
                log.info("server linkdown: {}", target);
            }
        });
        /** ping test */
        SendOnCloseByteBufHolder out = mss.sendMessage("abc");
        ByteBufs.writeString("12345", out.content());
        out.close();
        /** wait for quiet */
        waitQuiescent();
        assertTrue(clientRecv.get());
        assertTrue(serverRecv.get());
    }
}
