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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.meshy.ChannelMaster;
import com.addthis.meshy.ChannelState;
import com.addthis.meshy.Meshy;
import com.addthis.meshy.SourceHandler;

import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSource extends SourceHandler {

    protected static final Logger log = LoggerFactory.getLogger(FileTarget.class);
    static final boolean traceComplete = Parameter.boolValue("meshy.finder.debug.complete", false);

    private final LinkedList<FileReference> list = new LinkedList<>();
    private final String[] fileRequest;
    private FileReferenceFilter filter;

    public FileSource(ChannelMaster master, String files[]) {
        super(master, FileTarget.class);
        requestFiles("local", files);
        fileRequest = files;
    }

    public FileSource(ChannelMaster master, String files[], String scope) {
        super(master, FileTarget.class);
        requestFiles(scope, files);
        fileRequest = files;
    }

    public FileSource(ChannelMaster master, String files[], FileReferenceFilter filter) {
        super(master, FileTarget.class);
        this.filter = filter;
        requestFiles("local", files);
        fileRequest = files;
    }

    /**
     * used for internal proxy through mesh
     */
    public FileSource(ChannelMaster master, String targetUuid, String files[]) {
        super(master, FileTarget.class, targetUuid);
        requestFiles("remote", files);
        fileRequest = files;
    }

    @Override
    public String toString() {
        return super.toString() + '(' + (fileRequest != null ? Strings.join(fileRequest, ",") : "-") + ')';
    }

    private void requestFiles(String scope, String... matches) {
        send(Bytes.toBytes(scope));
        if (log.isDebugEnabled()) {
            log.debug(this + " scope=" + scope);
        }
        for (String match : matches) {
            if (log.isTraceEnabled()) {
                log.trace(this + " request=" + match);
            }
            send(Bytes.toBytes(match));
        }
        sendComplete();
    }

    public Collection<FileReference> getFileList() {
        return list;
    }

    public Map<String, FileReference> getFileMap() {
        HashMap<String, FileReference> map = new HashMap<>();
        for (FileReference file : getFileList()) {
            map.put(file.name, file);
        }
        return map;
    }

    @Override
    public void receive(int length, ChannelBuffer buffer) throws Exception {
        /* sync not required b/c overridden in server-server calls */
        FileReference ref = new FileReference(Meshy.getBytes(length, buffer));
        if (filter == null || filter.accept(ref)) {
            receiveReference(ref);
        }
        if (log.isTraceEnabled()) {
            log.trace(this + " recv=" + list.size());
        }
    }

    @Override
    public void receiveComplete(ChannelState state, int completedSession) throws Exception {
        if (traceComplete) {
            log.info("recv.complete [" + completedSession + "] " + Strings.join(fileRequest, ","));
        }
        super.receiveComplete(state, completedSession);
    }

    // override in subclasses for async handling
    // call super() if you still want the list populated
    public void receiveReference(FileReference ref) {
        list.add(ref);
    }

    // override in subclasses for async handling
    @Override
    public void receiveComplete() throws Exception {
        if (log.isDebugEnabled()) {
            log.debug(this + " recvComplete");
        }
    }
}
