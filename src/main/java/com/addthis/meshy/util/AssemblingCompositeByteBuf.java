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

package com.addthis.meshy.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;

public class AssemblingCompositeByteBuf extends CompositeByteBuf {

    public AssemblingCompositeByteBuf(ByteBufAllocator alloc, boolean direct, int maxNumComponents) {
        super(alloc, direct, maxNumComponents);
    }

    public AssemblingCompositeByteBuf(ByteBufAllocator alloc, boolean direct, int maxNumComponents, Iterable<ByteBuf> buffers) {
        super(alloc, direct, maxNumComponents, buffers);
    }

    public AssemblingCompositeByteBuf(ByteBufAllocator alloc, boolean direct, int maxNumComponents, ByteBuf... buffers) {
        super(alloc, direct, maxNumComponents, buffers);
    }

    public void writeComponent(ByteBuf buf) {
        addComponent(buf);
        writerIndex(writerIndex() + buf.readableBytes());
    }
}
