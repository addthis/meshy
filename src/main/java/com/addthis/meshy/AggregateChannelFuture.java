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

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ImmediateEventExecutor;

class AggregateChannelFuture extends DefaultPromise<Void> {

    public final Collection<ChannelFuture> futures;

    private final AtomicInteger complete;
    private final ChannelFutureListener aggregatingListener;

    // the group failure will just report any one sub-cause rather than bothering with a new exception
    private volatile Throwable anyCause = null;

    /** the promises collection should not be mutated after construction */
    AggregateChannelFuture(Collection<ChannelFuture> futures, EventExecutor executor) {
        super(executor);
        this.futures = futures;
        this.complete = new AtomicInteger(0);
        this.aggregatingListener = future -> {
            if (!future.isSuccess()) {
                anyCause = future.cause();
            }
            if (complete.incrementAndGet() == futures.size()) {
                if (anyCause == null) {
                    super.setSuccess(null);
                } else {
                    super.setFailure(anyCause);
                }
            }
        };
        for (ChannelFuture future : futures) {
            future.addListener(aggregatingListener);
        }
    }

    AggregateChannelFuture(Collection<ChannelFuture> futures) {
        this(futures, ImmediateEventExecutor.INSTANCE);
    }
}
