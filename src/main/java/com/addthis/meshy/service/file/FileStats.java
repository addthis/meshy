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
package com.addthis.meshy.service.file;


public class FileStats {

    public final int finds;
    public final int found;
    public final int findsRunning;
    public final int finderQueue;
    public final long findTime;
    public final long findTimeLocal;
    public final int cacheEvict;
    public final int cacheHit;

    public FileStats() {
        finds = FileTarget.finds.getAndSet(0);
        found = FileTarget.found.getAndSet(0);
        findsRunning = (int) FileTarget.findsRunning.count();
        finderQueue = FileTarget.finderQueue.size();
        findTime = FileTarget.findTime.getAndSet(0);
        findTimeLocal = FileTarget.findTimeLocal.getAndSet(0);
        cacheEvict = FileTarget.cacheEvicts.getAndSet(0);
        cacheHit = FileTarget.cacheHits.getAndSet(0);
    }
}
