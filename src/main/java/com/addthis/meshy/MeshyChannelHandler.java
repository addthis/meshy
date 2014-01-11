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

import java.net.ConnectException;

import java.nio.channels.ClosedChannelException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;

/**
 * one channel handler is created per meshy instance (client or server)
 */
class MeshyChannelHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(MeshyChannelHandler.class);
    private static final AttributeKey<ChannelState> STATE =
            AttributeKey.valueOf("MeshyChannel.state");

    private Meshy meshy;

    public MeshyChannelHandler(Meshy meshy) {
        this.meshy = meshy;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        meshy.updateLastEventTime();
        ctx.attr(STATE).get().channelRead(msg);
        ReferenceCountUtil.release(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        meshy.updateLastEventTime();
        if (cause instanceof ClosedChannelException) {
            log.warn("{} exception = {}", cause, ctx.attr(STATE).get());
        } else if (cause instanceof ConnectException) {
            // it is expected for the thread who requested the connection to report an unexpected failure
            log.debug("{} exception = {}", cause, ctx.attr(STATE).get());
        } else {
            log.warn("Netty exception caught. Closing channel. ChannelState: {}",
                    ctx.attr(STATE).get(), cause);
            ctx.channel().close();
        }
        try {
            channelInactive(ctx);
        } catch (Exception ee) {
            log.error("Mystery exception we are swallowing", ee);
        }
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) {
        ChannelState newState = new ChannelState(meshy, ctx.channel());
        ctx.attr(STATE).set(newState);
        log.trace("{} created for {}", newState, ctx.hashCode());
        meshy.updateLastEventTime();
        meshy.connectChannel(ctx.channel(), newState);
        newState.channelRegistered();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        meshy.updateLastEventTime();
        ctx.attr(STATE).get().channelClosed();
        meshy.closeChannel(ctx.channel());
    }
}
