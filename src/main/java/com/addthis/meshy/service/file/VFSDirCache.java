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

import java.util.LinkedHashMap;
import java.util.Map;

public final class VFSDirCache {

    /**
     * TODO caches are local to a stem root, but this cache is global.
     * unless you have multiple servers in a single VM, this is not an
     * issue.  for some JUnit tests, you have to clear the cache
     * between tests or they will fail.  This could be fixed with a
     * map of caches, but that's overkill for our uses.
     * TODO redo this whole cache. If too busy, maybe set the LHM to access order
     */
    final LinkedHashMap<String, VFSDirCacheLine> cache = new LinkedHashMap<String, VFSDirCacheLine>() {
        protected boolean removeEldestEntry(Map.Entry<String, VFSDirCacheLine> kvEntry) {
            if (size() > FileTarget.dirCacheSize) {
                FileTarget.cacheEvicts.incrementAndGet();
                FileTarget.cacheEvictsMeter.mark();
                return true;
            }
            return false;
        }
    };

    public void clear() {
        cache.clear();
    }

    public VFSDirCacheLine get(String path) {
        return cache.get(path);
    }

    public void put(String path, VFSDirCacheLine dirCache) {
        cache.put(path, dirCache);
    }
}
