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

public class StreamStats {

    public final int openCount;
    public final int newOpenCount;
    public final int readWaitTime;
    public final int sendWaiting;
    public final int sleeps;
    public final int qSize;
    public final int seqRead;
    public final int totalRead;
    public final int readBytes;
    public final int closedStreams;

    public StreamStats() {
        openCount = (int) StreamService.openStreams.count();
        newOpenCount = StreamService.newOpenStreams.getAndSet(0);
        readWaitTime = StreamService.readWaitTime.getAndSet(0);
        seqRead = StreamService.seqReads.getAndSet(0);
        totalRead = StreamService.totalReads.getAndSet(0);
        sendWaiting = StreamService.sendWaiting.get();
        sleeps = StreamService.sleeps.getAndSet(0);
        qSize = StreamTarget.senderQueue.size();
        readBytes = StreamService.readBytes.getAndSet(0);
        closedStreams = StreamService.closedStreams.getAndSet(0);
    }
}
