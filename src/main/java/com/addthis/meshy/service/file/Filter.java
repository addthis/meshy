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

import com.addthis.meshy.filesystem.VirtualFileFilter;
import com.addthis.meshy.filesystem.VirtualFileReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * simple matching: exact, all, begins with and ends with
 */
public class Filter implements VirtualFileFilter {

    private static final Logger log = LoggerFactory.getLogger(Filter.class);

    private String token;
    private boolean all;
    private boolean start;
    private boolean end;

    public Filter(final String token, final boolean all, final boolean start, final boolean end) {
        this.token = token;
        this.all = all;
        this.start = start;
        this.end = end;
    }

    @Override
    public String getToken() {
        return token;
    }

    @Override
    public boolean singleMatch() {
        return !(all || start || end);
    }

    @Override
    public String toString() {
        return "filter[tok=" + token + ",all=" + all + ",start=" + start + ",end=" + end + ']';
    }

    @Override
    public boolean accept(final VirtualFileReference ref) {
        final String fileName = ref.getName();
        final boolean ret = (all) ||
                            (start && fileName.startsWith(token)) ||
                            (end && fileName.endsWith(token)) ||
                            (fileName.equals(token));
        if (log.isTraceEnabled()) {
            log.trace("accept? (" + ref + ") = " + ret);
        }
        return ret;
    }
}
