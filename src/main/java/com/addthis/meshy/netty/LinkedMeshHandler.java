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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPromise;

/**
 * A relatively lightweight handler / pipeline connector.
 * <p/>
 * For now, do not use the ctx object, but instead call super.METHOD
 */
public class LinkedMeshHandler extends LinkedMeshInboundHandler implements ChannelOutboundHandler {

    public ChannelOutboundHandler nextOutbound;

    public LinkedMeshHandler(ChannelInboundHandler nextInbound, ChannelOutboundHandler nextOutbound) {
        super(nextInbound);
        this.nextOutbound = nextOutbound;
    }

    public LinkedMeshHandler(ChannelInboundHandler nextInbound) {
        super(nextInbound);
    }

    public LinkedMeshHandler(ChannelOutboundHandler nextOutbound) {
        super();
        this.nextOutbound = nextOutbound;
    }

    public LinkedMeshHandler() {
        super();
    }

    @Override
    public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) throws Exception {
        nextOutbound.bind(ctx, localAddress, promise);
    }

    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
        nextOutbound.connect(ctx, remoteAddress, localAddress, promise);
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        nextOutbound.disconnect(ctx, promise);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        nextOutbound.close(ctx, promise);
    }

    @Override
    @Deprecated
    public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        nextOutbound.deregister(ctx, promise);
    }

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        nextOutbound.read(ctx);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        nextOutbound.write(ctx, msg, promise);
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        nextOutbound.flush(ctx);
    }
}
