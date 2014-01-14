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
import java.io.FileInputStream;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import java.nio.file.Files;
import java.nio.file.Path;

import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.meshy.filesystem.VirtualFileFilter;
import com.addthis.meshy.filesystem.VirtualFileInput;
import com.addthis.meshy.filesystem.VirtualFileReference;
import com.addthis.meshy.filesystem.VirtualFileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LocalFileSystem implements VirtualFileSystem {

    private static final Logger log = LoggerFactory.getLogger(LocalFileSystem.class);

    private static LocalFileHandler handlers[];

    static {
        reloadHandlers();
    }

    public static void reloadHandlers() {
        LinkedList<LocalFileHandler> list = new LinkedList<>();
        if (LocalFileHandlerMux.muxEnabled) {
            list.add(new LocalFileHandlerMux());
        }
        String handlerClasses[] = Strings.splitArray(Parameter.value("mesh.local.handlers", ""), ",");
        for (String handler : handlerClasses) {
            try {
                list.add((LocalFileHandler) (Class.forName(handler).newInstance()));
            } catch (Exception ex) {
                log.warn("unable to load file handler: ", ex);
            }
        }
        handlers = list.toArray(new LocalFileHandler[list.size()]);
    }

    private FileReference rootDir;

    public LocalFileSystem(File rootDir) {
        this.rootDir = new FileReference(rootDir);
    }

    @Override
    public String toString() {
        return "VFS:" + rootDir;
    }

    @Override
    public String[] tokenizePath(String path) {
        return Strings.splitArray(path, "/");
    }

    @Override
    public VirtualFileReference getFileRoot() {
        return rootDir;
    }

    /**
     * normal ptr reference
     */
    private static final class FileReference implements VirtualFileReference {

        private final File ptr;

        FileReference(final File file) {
            this.ptr = file;
        }

        @Override
        public String toString() {
            return "VFR:" + ptr;
        }

        @Override
        public String getName() {
            return ptr.getName();
        }

        @Override
        public long getLastModified() {
            return ptr.lastModified();
        }

        @Override
        public long getLength() {
            return ptr.length();
        }

        @Override
        public Iterator<VirtualFileReference> listFiles(final VirtualFileFilter filter) {
            try {
                return listFilesHelper(filter);
            } catch (Exception ex) {
                log.error("Mystery exception we are swallowing", ex);
                return null;
            }
        }

        @Override
        public VirtualFileReference getFile(String name) {
            for (LocalFileHandler handler : handlers) {
                if (handler.canHandleDirectory(ptr)) {
                    return handler.getFile(ptr, name);
                }
            }
            File next = new File(ptr, name);
            return next.exists() ? new FileReference(next) : null;
        }

        /**
         * unsafe. catch delegated to wrapper
         */
        private Iterator<VirtualFileReference> listFilesHelper(final VirtualFileFilter filter) throws Exception {
            for (LocalFileHandler handler : handlers) {
                if (handler.canHandleDirectory(ptr)) {
                    if (log.isDebugEnabled()) {
                        log.debug("delegate " + ptr + " to " + handler);
                    }
                    return handler.listFiles(ptr, filter);
                }
            }
            if (filter != null && filter.singleMatch()) {
                Path path = ptr.toPath().resolve(filter.getToken());
                if (Files.exists(path)) {
                    LinkedList<VirtualFileReference> list = new LinkedList<>();
                    list.add(new FileReference(path.toFile()));
                    return list.iterator();
                }
            }
            File files[] = ptr.listFiles();
            if (files == null) {
                if (log.isDebugEnabled()) {
                    log.debug("no files for " + ptr);
                }
                return null;
            }
            LinkedList<VirtualFileReference> list = new LinkedList<>();
            for (File file : files) {
                VirtualFileReference ref = new FileReference(file);
                if (filter == null || filter.accept(ref)) {
                    list.add(ref);
                }
            }
            if (log.isDebugEnabled()) {
                log.debug(ptr + " matched " + list.size());
            }
            return list.iterator();
        }

        @Override
        public VirtualFileInput getInput(final Map<String, String> options) {
            try {
                if (ptr.isFile() && ptr.canRead()) {
                    return new InputStreamWrapper(new FileInputStream(ptr));
                }
            } catch (Exception ex) {
                log.error("Mystery exception we are swallowing", ex);
            }
            return null;
        }
    }
}
