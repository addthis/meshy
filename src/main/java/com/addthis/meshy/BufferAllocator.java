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

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.buffer.DirectChannelBufferFactory;
import org.jboss.netty.buffer.HeapChannelBufferFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
 * TODO: I believe there is an accounting issue under load where
 * buffer returns are missed.  this results in poor performance as the
 * buffer cache throttles new buffer creation
 */
public class BufferAllocator {

    private static final Logger log = LoggerFactory.getLogger(BufferAllocator.class);

    static final long MAX_CACHE_SIZE = Parameter.longValue("meshy.buffers.max", 100) * 1024 * 1024;
    static final long CACHE_OVERAGE_SLEEP = Parameter.longValue("meshy.buffers.slowdown", 0);
    static final long MAX_POOL_ENTRIES = Parameter.longValue("meshy.pool.entry.max", 2500);
    static final long MAX_POOL_MEM = Parameter.longValue("meshy.pool.mem.max", 5) * 1024 * 1024;
    static final boolean ENABLED = Parameter.boolValue("meshy.buffers.enable", false);
    static final boolean DIRECT = Parameter.boolValue("meshy.buffers.direct", false);
    static final int BASE_SIZE = 64;

    private final ChannelBufferFactory bufferFactory;
    private final SizedPool pools[];
    private final AtomicLong bufferMem = new AtomicLong(0); // buffer mem in cache
    private final AtomicLong bufferOut = new AtomicLong(0); // buffer mem leased to apps
    private final AtomicInteger leases = new AtomicInteger(0); // buffer objects leased (timed/reset)
    private final AtomicInteger returns = new AtomicInteger(0); // buffer objects returned (timed/reset)
    private final AtomicInteger sleeps = new AtomicInteger(0); // threshold induced sleeps (timed/reset)
    private final AtomicInteger leaseOut = new AtomicInteger(0); // outstanding leased buffers

    BufferAllocator() {
        bufferFactory = DIRECT ? new DirectChannelBufferFactory() : new HeapChannelBufferFactory();
        pools = new SizedPool[24];
        // init pools
        int size = BASE_SIZE;
        for (int i = 0; i < pools.length; i++) {
            pools[i] = new SizedPool(size);
            size *= 2;
        }
        if (log.isDebugEnabled()) {
            log.debug("BufferCache: enabled=" + ENABLED + " direct=" + DIRECT + " max=" + MAX_CACHE_SIZE + " sleep=" + CACHE_OVERAGE_SLEEP + " pools=" + Strings.join(pools, ","));
        }
    }

    public ChannelBuffer allocateBuffer(int size) {
        if (!ENABLED) {
            return bufferFactory.getBuffer(size);
        }
        if (log.isTraceEnabled()) {
            log.trace("allocate buffer " + size);
        }
        for (SizedPool pool : pools) {
            if (size <= pool.size) {
                return pool.allocateBuffer();
            }
        }
        throw new RuntimeException("excessive allocation; " + size);
    }

    public void returnBuffer(ChannelBuffer done) {
        if (!ENABLED) {
            return;
        }
        int capacity = done.capacity();
        for (int i = 0; i < pools.length; i++) {
            if (capacity == pools[i].size) {
                if (log.isTraceEnabled()) {
                    log.debug("return buffer " + capacity + " [" + i + "]");
                }
                pools[i].returnBuffer(done);
                return;
            }
        }
        throw new RuntimeException("invalid return size: " + capacity);
    }

    private class SizedPool {

        public final int size;
        public final LinkedList<ChannelBuffer> buffers;
        public final AtomicInteger allocations = new AtomicInteger(0);

        SizedPool(int size) {
            this.size = size;
            this.buffers = new LinkedList<>();
        }

        @Override
        public String toString() {
            return size + ":" + buffers.size();
        }

        public ChannelBuffer allocateBuffer() {
            allocations.incrementAndGet();
            leases.incrementAndGet();
            leaseOut.incrementAndGet();
            long newSize = bufferOut.addAndGet(size);
            if (MAX_CACHE_SIZE > 0 && newSize > MAX_CACHE_SIZE) {
                sleeps.incrementAndGet();
                if (CACHE_OVERAGE_SLEEP > 0) {
                    try {
                        Thread.sleep(CACHE_OVERAGE_SLEEP);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                } else {
                    Thread.yield();
                }
            }
            synchronized (this) {
                if (!buffers.isEmpty()) {
                    bufferMem.addAndGet(-size);
                    return buffers.removeFirst();
                }
            }
            return bufferFactory.getBuffer(size);
        }

        public void returnBuffer(ChannelBuffer buffer) {
            if (buffer.capacity() != size) {
                throw new RuntimeException("invalid return size: " + buffer.capacity() + " vs " + size);
            }
            returns.incrementAndGet();
            leaseOut.decrementAndGet();
            bufferOut.addAndGet(-size);
            // drop here if pool is oversized
            synchronized (this) {
                if (buffers.size() < MAX_POOL_ENTRIES && buffers.size() * size < MAX_POOL_MEM) {
                    bufferMem.addAndGet(size);
                    buffer.clear();
                    buffers.addLast(buffer);
                }
            }
        }
    }

    public Stats getStats() {
        return new Stats(this);
    }

    public static class Stats {

        public final long bufferOut;
        public final long bufferMem;
        public final int leaseOut;
        public final int leases;
        public final int returns;
        public final int sleeps;
        public final Integer poolSizes[];
        public final Integer poolLeases[];

        private Stats(BufferAllocator bufferAllocator) {
            this.bufferMem = bufferAllocator.bufferMem.get();
            this.bufferOut = bufferAllocator.bufferOut.get();
            this.sleeps = bufferAllocator.sleeps.getAndSet(0);
            this.leases = bufferAllocator.leases.getAndSet(0);
            this.returns = bufferAllocator.returns.getAndSet(0);
            this.leaseOut = bufferAllocator.leaseOut.getAndSet(0);
            poolSizes = new Integer[bufferAllocator.pools.length];
            poolLeases = new Integer[bufferAllocator.pools.length];
            for (int i = 0; i < poolSizes.length; i++) {
                poolSizes[i] = bufferAllocator.pools[i].buffers.size();
                poolLeases[i] = bufferAllocator.pools[i].allocations.getAndSet(0);
            }
        }
    }
}
