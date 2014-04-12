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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import java.nio.CharBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.CharsetUtil;

public class ByteBufs {

    public static ByteBufAllocator meshAlloc = PooledByteBufAllocator.DEFAULT;

    public static ByteBuf fromString(String string) {
        return ByteBufUtil.encodeString(meshAlloc, CharBuffer.wrap(string), CharsetUtil.UTF_8);
    }

    public static String toString(ByteBuf buf) {
        return buf.toString(CharsetUtil.UTF_8);
    }

    public static ByteBuf quickAlloc() {
        return meshAlloc.buffer();
    }

    public static ByteBufOutputStream allocOutputStream() {
        return new ByteBufOutputStream(meshAlloc.buffer());
    }

    public static void writeString(String string, ByteBuf to) {
        ByteBuf stringBuf = ByteBufs.fromString(string);
        writeLength((long) stringBuf.readableBytes(), to);
        to.writeBytes(stringBuf);
    }

    public static String readString(ByteBuf from) throws IOException {
        long length = readLength(from);
        ByteBuf stringSlice = from.readSlice((int) length);
        return toString(stringSlice);
    }

    public static byte[] readBytes(ByteBuf from) throws IOException {
        long length = readLength(from);
        ByteBuf slice = from.readSlice((int) length);
        byte[] bytes = new byte[slice.readableBytes()];
        slice.readBytes(bytes);
        return bytes;
    }

    public static void writeBytes(byte[] bytes, ByteBuf to) throws IOException {
        long length = bytes.length;
        writeLength(length, to);
        to.writeBytes(bytes);
    }

    public static byte[] toBytes(ByteBuf buf) {
        int length = buf.readableBytes();
        byte[] rawBytes = new byte[length];
        buf.readBytes(rawBytes);
        return rawBytes;
    }

    /**
     * Read an InputStream to it's end and return as a ByteBuf
     *
     * @param in
     * @return
     * @throws IOException
     */
    public static ByteBuf readFully(InputStream in) throws IOException {
        ByteBuf buf = ByteBufs.quickAlloc();
        while (buf.writeBytes(in, 1024) >= 0) {
            // read more
        }
        return buf;
    }

    /**
     * Write a length field to a ByteBuf.
     *
     * @param size
     * @param buf
     * @throws IOException
     */
    public static void writeLength(long size, ByteBuf buf) {
        if (size < 0) {
            throw new IllegalArgumentException("writeLength value must be >= 0: " + size);
        }
        if (size == 0) {
            buf.writeByte(0);
            return;
        }
        while (size > 0) {
            if (size > 0x7f) {
                buf.writeByte((int) (0x80 | (size & 0x7f)));
            } else {
                buf.writeByte((int) (size & 0x7f));
            }
            size >>= 7;
        }
    }

    /**
     * Read a length field from a ByteBuf
     *
     * @param in
     * @return
     * @throws IOException
     */
    public static long readLength(ByteBuf in) throws IOException {
        long size = 0;
        long iter = 0;
        long next = 0;
        do {
            if (!in.isReadable()) {
                throw new EOFException();
            }
            next = in.readByte();
            size |= ((next & 0x7f) << iter);
            iter += 7;
        }
        while ((next & 0x80) == 0x80);
        return size;
    }
}
