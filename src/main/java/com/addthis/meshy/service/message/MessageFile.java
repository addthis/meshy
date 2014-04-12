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
package com.addthis.meshy.service.message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import com.addthis.basis.util.JitterClock;

import com.addthis.meshy.filesystem.VirtualFileFilter;
import com.addthis.meshy.filesystem.VirtualFileInput;
import com.addthis.meshy.filesystem.VirtualFileReference;

class MessageFile implements VirtualFileReference {

    private long lastModified;
    private final String name;
    private final long length;
    private final HashMap<String, MessageFile> files = new HashMap<>();

    MessageFile(String name, long lastModified, long length) {
        this.name = name;
        this.lastModified = lastModified;
        this.length = length;
    }

    void addFile(String fileName, MessageFile file) {
        synchronized (files) {
            files.put(fileName, file);
        }
        lastModified = JitterClock.globalTime();
    }

    void removeFile(String fileName) {
        synchronized (files) {
            files.remove(fileName);
        }
        lastModified = JitterClock.globalTime();
    }

    void removeFiles(final TopicSender target) {
        LinkedList<String> names = new LinkedList<>();
        synchronized (files) {
            for (Map.Entry<String, MessageFile> e : files.entrySet()) {
                MessageFile mf = e.getValue();
                // TODO this probably isn't good
                if (mf instanceof MessageFileListener && ((MessageFileListener) mf).target == target) {
                    names.add(e.getKey());
                } else {
                    mf.removeFiles(target);
                }
            }
        }
        for (String fileName : names) {
            removeFile(fileName);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getLastModified() {
        return lastModified;
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public Iterator<VirtualFileReference> listFiles(VirtualFileFilter filter) {
        synchronized (files) {
            if (files.isEmpty()) {
                return null;
            }
            ArrayList<VirtualFileReference> filtered = new ArrayList<>();
            for (MessageFile file : files.values()) {
                if (filter.accept(file)) {
                    filtered.add(file);
                }
            }
            return filtered.iterator();
        }
    }

    @Override
    public VirtualFileReference getFile(String fileName) {
        synchronized (files) {
            return files.get(fileName);
        }
    }

    @Override
    public VirtualFileInput getInput(Map<String, String> options) {
        return null;
    }
}
