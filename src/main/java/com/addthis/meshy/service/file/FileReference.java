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

import java.io.IOException;

import com.addthis.basis.util.Bytes;

import com.addthis.meshy.filesystem.VirtualFileReference;
import com.addthis.meshy.util.ByteBufs;

import com.google.common.base.Objects;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

public class FileReference {

    public final String name;
    public final long lastModified;
    public final long size;

    private String hostUUID;

    public FileReference(final String name, final long last, final long size) {
        this.name = name;
        this.lastModified = last;
        this.size = size;
    }

    public FileReference(final String prefix, final VirtualFileReference ref) {
        this(prefix + '/' + ref.getName(), ref.getLastModified(), ref.getLength());
    }

    public FileReference(ByteBuf data) throws IOException {
        ByteBufInputStream in = new ByteBufInputStream(data);
        name = Bytes.readString(in);
        lastModified = Bytes.readLength(in);
        size = Bytes.readLength(in);
        hostUUID = Bytes.readString(in);
    }

    /**
     * should only be used by the test harness
     */
    protected FileReference setHostUUID(final String uuid) {
        this.hostUUID = uuid;
        return this;
    }

    public String getHostUUID() {
        return hostUUID;
    }

    void encode(String uuid, ByteBuf to) {
        to.writeBytes(ByteBufs.fromString(name));
        ByteBufs.writeLength(lastModified, to);
        ByteBufs.writeLength(size, to);
        to.writeBytes(ByteBufs.fromString((uuid != null) ? uuid : hostUUID));
    }

    ByteBuf encode(String uuid) {
        ByteBuf to = ByteBufs.MESH_ALLOC.buffer();
        encode(uuid, to);
        return to;
    }

    @Override
    public String toString() {
        return "[nm=" + name + ",lm=" + lastModified + ",sz=" + size + ",uu=" + hostUUID + ']';
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FileReference)) {
            return false;
        }
        FileReference otherReference = (FileReference) other;
        if (this.name == null && otherReference.name != null) {
            return false;
        }
        if (name != null && !name.equals(otherReference.name)) {
            return false;
        }
        if (lastModified != otherReference.lastModified) {
            return false;
        }
        if (size != otherReference.size) {
            return false;
        }
        if (this.hostUUID == null && otherReference.hostUUID != null) {
            return false;
        }
        if (hostUUID != null && !hostUUID.equals(otherReference.hostUUID)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, lastModified, size, hostUUID);
    }
}
