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

import java.io.File;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import com.addthis.meshy.filesystem.VirtualFileFilter;
import com.addthis.meshy.filesystem.VirtualFileInput;
import com.addthis.meshy.filesystem.VirtualFileReference;
import com.addthis.muxy.ReadMuxFile;
import com.addthis.muxy.ReadMuxFileDirectory;
import com.addthis.muxy.ReadMuxFileDirectoryCache;

import com.google.common.base.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LocalFileHandlerMux implements LocalFileHandler {

    private static final Logger log = LoggerFactory.getLogger(LocalFileHandlerMux.class);

    private static boolean checkForMux() {
        try {
            Class.forName("com.addthis.muxy.ReadMuxFileDirectory");
            log.info("Muxy class path found and loaded.");
            return true;
        } catch (ClassNotFoundException cnfe) {
            log.warn("Muxy not found in path and not loaded.");
            return false;
        }
    }

    public static final boolean muxEnabled = !Boolean.getBoolean("meshy.muxy.disable") && checkForMux();

    @Override
    public boolean canHandleDirectory(File dir) {
        return ReadMuxFileDirectory.isMuxDir(dir.toPath());
    }

    @Override
    public Iterator<VirtualFileReference> listFiles(File dir, VirtualFileFilter filter) {
        try {
            LinkedList<VirtualFileReference> list = new LinkedList<>();
            for (ReadMuxFile meta : ReadMuxFileDirectoryCache.listFiles(dir)) {
                VirtualFileReference ref = new MuxFileReference(meta);
                if (filter == null || filter.accept(ref)) {
                    list.add(ref);
                }
            }
            return list.iterator();
        } catch (Exception ex) {
            log.error("Mystery exception we are swallowing", ex);
            return null;
        }
    }

    @Override
    public VirtualFileReference getFile(File dir, String name) {
        try {
            return new MuxFileReference(ReadMuxFileDirectoryCache.getFileMeta(dir, name));
        } catch (Exception ex) {
            log.error("Mystery exception we are swallowing", ex);
            return null;
        }
    }

    /**
     * multiplexed ptr reference
     */
    private static final class MuxFileReference implements VirtualFileReference {

        private final ReadMuxFile meta;

        MuxFileReference(ReadMuxFile meta) {
            this.meta = meta;
        }

        @Override
        public String getName() {
            return meta.getName();
        }

        @Override
        public long getLastModified() {
            return meta.getLastModified();
        }

        @Override
        public long getLength() {
            return meta.getLength();
        }

        /**
         * mux files cannot have sub-directories
         */
        @Override
        public Iterator<VirtualFileReference> listFiles(VirtualFileFilter filter) {
            return null;
        }

        @Override
        public VirtualFileReference getFile(String name) {
            return null;
        }

        @Override
        public VirtualFileInput getInput(Map<String, String> options) {
            try {
                return new InputStreamWrapper(meta.read(0));
            } catch (Exception e) {
                log.error("Mystery exception we are swallowing", e);
                return null;
            }
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("meta", meta)
                    .toString();
        }
    }
}
