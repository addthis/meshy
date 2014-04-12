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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.addthis.basis.util.Parameter;

import com.addthis.meshy.filesystem.VirtualFileInput;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

/**
 * simple wrapper.  does not attempt to follow nextMessage() wait
 * contract.  assumption is this is wrapping a file input or
 * similar that won't block for long.
 */
public class InputStreamWrapper implements VirtualFileInput {

    private static final Meter shortReadMeter = Metrics.newMeter(InputStreamWrapper.class, "shortReads", "shortReads", TimeUnit.SECONDS);
    private static final AtomicLong shortRead = new AtomicLong(0);
    private static final int DEFAULT_BUFFER = Parameter.intValue("meshy.input.wrapper.buffer", 16000);
    private static final int DEFAULT_MINIMUM = Parameter.intValue("meshy.input.wrapper.bufferMin", 16000);

    public static long getShortReadCount() {
        return shortRead.getAndSet(0);
    }

    private final byte[] buf;
    private final InputStream input;
    private boolean done;

    public InputStreamWrapper(InputStream input) {
        /* leave overhead for framing when creating wrapper buffers (powers of 2) */
        this(input, DEFAULT_BUFFER);
    }

    public InputStreamWrapper(InputStream input, int bufSize) {
        this.input = input;
        this.buf = new byte[bufSize];
    }

    public byte[] readBytes(InputStream in, byte[] b, int min) throws IOException {
        int got = 0;
        int read = 0;
        while (got < min && (read = in.read(b, got, b.length - got)) >= 0) {
            got += read;
        }
        if (read < 0) {
            done = true;
        }
        if (got < b.length) {
            byte ret[] = new byte[got];
            System.arraycopy(b, 0, ret, 0, got);
            b = ret;
            shortRead.incrementAndGet();
            shortReadMeter.mark();
        }
        return b;
    }

    @Override
    public Object nextMessage(long wait) {
        if (done) {
            return null;
        }
        try {
            if (DEFAULT_MINIMUM > 0) {
                return readBytes(input, buf, DEFAULT_MINIMUM);
            }
            int read = input.read(buf);
            if (read < 0) {
                done = true;
                return null;
            }
            if (read < buf.length) {
                shortReadMeter.mark();
                shortRead.incrementAndGet();
                return Arrays.copyOf(buf, read);
            }
            return buf;
        } catch (EOFException ex) {
            done = true;
            return null;
        } catch (Exception ex) {
            ex.printStackTrace();
            done = true;
            return null;
        }
    }

    @Override
    public boolean isEOF() {
        return done;
    }

    @Override
    public void close() {
        try {
            input.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
