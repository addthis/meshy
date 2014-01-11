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

import io.netty.buffer.ByteBuf;


public interface SessionHandler {

    public boolean send(byte data[], SendWatcher watcher);

    public boolean sendComplete();

    public void receive(ChannelState state, int session, int length, ByteBuf buffer) throws Exception;

    public void receiveComplete(ChannelState state, int session) throws Exception;

    public void receiveComplete(int session) throws Exception;

    public void waitComplete();
}
