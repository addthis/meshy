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
package com.addthis.meshy.service.stream;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.basis.util.Parameter;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StreamService {

    protected static final Logger log = LoggerFactory.getLogger(StreamService.class);

    public static final String ERROR_EXCEED_OPEN = "Exceeded Max Open Files";
    public static final String ERROR_CHANNEL_LOST = "Channel Connection Lost";

    static final int MODE_START = 0;
    static final int MODE_MORE = 1;
    static final int MODE_FAIL = 2;
    static final int MODE_CLOSE = 3;
    static final int MODE_START_2 = 4; // passing options

    /* secret byte handshake */
    static final byte[] FAIL_BYTES = new byte[0];
    /* bigger buffers = better performance, chance of OOMing a heavily loaded/peered meshy node */
    /* pre-fetch stream on open */
    static final boolean DIRECT_COPY = Parameter.boolValue("meshy.copy.direct", true);
    /* log dropped "more" requests */
    static final boolean LOG_DROP_MORE = Parameter.boolValue("meshy.log.dropmore", false);
    /* max time to wait for a VirtualFileInput read() call in sender threads */
    static final long READ_WAIT = Parameter.longValue("meshy.read.wait", 10);
    /* for enforcing MAX_OPEN_STREAMS */
    static final Counter openStreams = Metrics.newCounter(StreamService.class, "openStreams");
    static final Meter newStreamMeter = Metrics.newMeter(StreamService.class, "newStreams", "newStreams", TimeUnit.SECONDS);
    static final AtomicInteger newOpenStreams = new AtomicInteger(0);
    static final AtomicInteger closedStreams = new AtomicInteger(0);
    static final AtomicInteger readBytes = new AtomicInteger(0);
    static final AtomicInteger seqReads = new AtomicInteger(0);
    static final AtomicInteger totalReads = new AtomicInteger(0);
    static final AtomicInteger readWaitTime = new AtomicInteger(0);
    static final AtomicInteger sendWaiting = new AtomicInteger(0);
    static final AtomicInteger sleeps = new AtomicInteger(0);
}
