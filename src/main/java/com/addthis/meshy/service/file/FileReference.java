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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.addthis.basis.util.Bytes;

import com.addthis.meshy.VirtualFileReference;

import com.google.common.base.Objects;

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

    public FileReference(final byte[] data) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
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

    byte[] encode(String uuid) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(name.length() * 2 + 12);
            Bytes.writeString(name, out);
            Bytes.writeLength(lastModified, out);
            Bytes.writeLength(size, out);
            Bytes.writeString(uuid != null ? uuid : hostUUID, out);
            return out.toByteArray();
        } catch (IOException ie) {
            //using ByteArrayOutputStream. Cant actually throw these
            return null;
        }
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
        if (!Objects.equal(name, otherReference.name)) {
            return false;
        }
        if (lastModified != otherReference.lastModified) {
            return false;
        }
        if (size != otherReference.size) {
            return false;
        }
        if (!Objects.equal(hostUUID, otherReference.hostUUID)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, lastModified, size, hostUUID);
    }
}
