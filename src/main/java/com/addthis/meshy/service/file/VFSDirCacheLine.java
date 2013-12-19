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

import java.util.LinkedList;

import com.addthis.basis.util.JitterClock;

import com.addthis.meshy.VirtualFileReference;

public final class VFSDirCacheLine {

    final LinkedList<FileReference> lines = new LinkedList<>();

    private final VirtualFileReference dir;
    private final long created;
    private final long lastModified;

    public VFSDirCacheLine(VirtualFileReference dir) {
        this.dir = dir;
        this.created = JitterClock.globalTime();
        this.lastModified = dir.getLastModified();
    }

    boolean isValid() {
        return dir.getLastModified() == lastModified && (JitterClock.globalTime() - created < FileTarget.dirCacheAge);
    }

    @Override
    public String toString() {
        return "CL(" + dir + ")[" + lines.size() + "]{" + isValid() + '}';
    }
}
