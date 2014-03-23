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
package com.addthis.meshy.netty;

import java.net.SocketAddress;

import java.util.AbstractSet;
import java.util.Iterator;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;

/**
 * This is the world we live in
 */
public class DummyChannelGroup extends AbstractSet<Channel> implements ChannelGroup {

    public static final ChannelGroup DUMMY = new DummyChannelGroup();

    private DummyChannelGroup() {

    }

    public static ChannelGroup getDummy() {
        return DUMMY;
    }

    @Override
    public Iterator iterator() {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean add(Channel channel) {
        return false;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public Channel find(Integer id) {
        return null;
    }

    @Override
    public ChannelGroupFuture setInterestOps(int interestOps) {
        return null;
    }

    @Override
    public ChannelGroupFuture setReadable(boolean readable) {
        return null;
    }

    @Override
    public ChannelGroupFuture write(Object message) {
        return null;
    }

    @Override
    public ChannelGroupFuture write(Object message, SocketAddress remoteAddress) {
        return null;
    }

    @Override
    public ChannelGroupFuture disconnect() {
        return null;
    }

    @Override
    public ChannelGroupFuture unbind() {
        return null;
    }

    @Override
    public ChannelGroupFuture close() {
        return null;
    }

    @Override
    public int compareTo(ChannelGroup o) {
        return 0;
    }

}
