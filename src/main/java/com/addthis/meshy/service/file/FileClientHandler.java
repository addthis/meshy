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
package com.addthis.meshy.service.file;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.meshy.ChannelMaster;
import com.addthis.meshy.ChannelState;
import com.addthis.meshy.util.AssemblingCompositeByteBuf;
import com.addthis.meshy.util.ByteBufs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;

public class FileClientHandler extends ChannelDuplexHandler {

    protected static final Logger log = LoggerFactory.getLogger(FileClientHandler.class);
    static final boolean traceComplete = Parameter.boolValue("meshy.finder.debug.complete", false);

    private final LinkedList<FileReference> list = new LinkedList<>();

    ChannelHandlerContext ctx;

    private final String scope;
    private final FileReferenceFilter filter;
    private final String targetUuid;
    private final String[] files;

    public FileClientHandler(String files[]) {
        this("local", files);
    }

    public FileClientHandler(String files[], String scope) {
        this(null, files, scope, null);
    }

    public FileClientHandler(String files[], FileReferenceFilter filter) {
        this(null, files, "local", filter);
    }

    /**
     * used for internal proxy through mesh
     */
    public FileClientHandler(String targetUuid, String files[]) {
        this(targetUuid, files, "remote", null);
    }

    public FileClientHandler(String targetUuid, String files[], String scope, FileReferenceFilter filter) {
        this.targetUuid = targetUuid;
        this.files = files;
        this.scope = scope;
        this.filter = filter;
    }

    @Override
    public String toString() {
        return super.toString() + '(' + (files != null ? Strings.join(files, ",") : "-") + ')';
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
    }

    private void requestFiles(String scope, String... matches) {
        AssemblingCompositeByteBuf msg = ByteBufs.assemblingBuffer();
        msg.writeComponent(ByteBufs.fromString(scope));
        log.debug("{} scope={}", this, scope);
        for (String match : matches) {
            log.trace("{} request={}", this, match);
            msg.writeComponent(ByteBufs.fromString(match));
        }
        ctx.writeAndFlush(msg);
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

    public void receive(ByteBuf buffer) throws Exception {
        /* sync not required b/c overridden in server-server calls */
        FileReference ref = new FileReference(buffer);
        if (filter == null || filter.accept(ref)) {
            receiveReference(ref);
        }
        log.trace("{} recv={}", this, list.size());
    }

    public void receiveComplete(ChannelState state, int completedSession) throws Exception {
        if (traceComplete) {
            log.info("recv.complete [{}] {}", completedSession, Strings.join(files, ","));
        }
    }

    // override to detect unexpected channel closures
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
    }

    // override in subclasses for async handling
    // call super() if you still want the list populated
    public void receiveReference(FileReference ref) {
        list.add(ref);
    }

    // override in subclasses for async handling
    @Override
    public void receiveComplete() throws Exception {
        log.debug("{} recvComplete", this);
    }
}
