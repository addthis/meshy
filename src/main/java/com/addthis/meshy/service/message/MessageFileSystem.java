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

import java.io.IOException;
import java.io.InputStream;

import java.util.concurrent.atomic.AtomicLong;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Strings;

import com.addthis.meshy.VirtualFileReference;
import com.addthis.meshy.VirtualFileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MessageFileSystem implements VirtualFileSystem, TargetListener {

    static final Logger log = LoggerFactory.getLogger(MessageFileSystem.class);

    /**
     * file open option that overrides system timeout
     */
    public static final String READ_TIMEOUT = "mfs.read.timeout";

    static final String MFS_ADD = "mfs.add";
    static final String MFS_DEL = "mfs.del";

    static final AtomicLong nextReplyID = new AtomicLong(1);


    public MessageFileSystem() {
        root = new MessageFile("", JitterClock.globalTime(), 0);
        MessageTarget.registerListener(MFS_ADD, this);
        MessageTarget.registerListener(MFS_DEL, this);
    }

    private final MessageFile root;

    @Override
    public String[] tokenizePath(String path) {
        return Strings.splitArray(path, "/");
    }

    @Override
    public VirtualFileReference getFileRoot() {
        return root;
    }

    /**
     * for JVM internal implementations
     */
    public void addPath(String path, TopicSender sender) {
        updatePath(sender, path, true);
    }

    /**
     * for JVM internal implementations
     */
    public void removePath(String path) {
        updatePath(null, path, false);
    }

    private void updatePath(TopicSender target, String fullPath, boolean add) {
        String[] path = Strings.splitArray(fullPath, "/");
        MessageFile ptr = root;
        for (int i = 0; i < path.length; i++) {
            String tok = path[i];
            if (i == path.length - 1) {
                if (add) {
                    ptr.addFile(tok, new MessageFileListener(tok, fullPath, target));
                } else {
                    ptr.removeFile(tok);
                }
                return;
            }
            MessageFile next = (MessageFile) ptr.getFile(tok);
            if (next == null) {
                if (!add) {
                    return;
                }
                next = new MessageFile(tok, JitterClock.globalTime(), 0);
                ptr.addFile(tok, next);
            }
            ptr = next;
        }
    }

    @Override
    public void receiveMessage(TopicSender target, String topic, InputStream in) throws IOException {
        boolean add = topic.equals(MFS_ADD);
        boolean del = !add && topic.equals(MFS_DEL);
        if (add || del) {
            String fullPath = Bytes.readString(in);
            updatePath(target, fullPath, add);
        } else {
            log.warn("unhandled receive for topic=" + topic + " target=" + target);
        }
    }

    @Override
    public void linkDown(TopicSender target) {
        root.removeFiles(target);
    }
}
