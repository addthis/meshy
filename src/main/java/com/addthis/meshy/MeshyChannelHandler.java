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

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** one channel handler is created per meshy instance (client or server) */
class MeshyChannelHandler extends SimpleChannelHandler {
    private static final Logger log = LoggerFactory.getLogger(MeshyChannelHandler.class);

    private final Meshy meshy;

    public MeshyChannelHandler(Meshy meshy) {
        this.meshy = meshy;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent msg) {
        meshy.updateLastEventTime();
        getAttachState(ctx).messageReceived(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent ex) {
        meshy.updateLastEventTime();
        if (ex.getCause() instanceof ClosedChannelException) {
            log.warn("{} exception = {}", ex, ctx.getAttachment());
        } else if (ex.getCause() instanceof ConnectException) {
            // it is expected for the thread who requested the connection to report an unexpected failure
            log.debug("{} exception = {}", ex, ctx.getAttachment());
        } else {
            log.warn("Netty exception caught. Closing channel. ChannelState: {}",
                    ctx.getAttachment(), ex.getCause());
            ctx.getChannel().close();
        }
        try {
            channelClosed(ctx, null);
        } catch (Exception ee) {
            log.error("Mystery exception we are swallowing", ee);
        }
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        meshy.updateLastEventTime();
        meshy.channelConnected(ctx.getChannel(), getAttachState(ctx));
        getAttachState(ctx).channelConnected(e);
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        meshy.updateLastEventTime();
        getAttachState(ctx).channelClosed(e);
        meshy.channelClosed(ctx.getChannel());
    }

    private ChannelState getAttachState(ChannelHandlerContext ctx) {
        synchronized (ctx) {
            ChannelState state = (ChannelState) ctx.getAttachment();
            if (state == null) {
                state = new ChannelState(meshy, ctx.getChannel());
                log.trace("{} created for {}", state, ctx.hashCode());
                ctx.setAttachment(state);
            }
            return state;
        }
    }
}
