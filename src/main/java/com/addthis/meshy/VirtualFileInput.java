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
package com.addthis.meshy;


public interface VirtualFileInput {

    /**
     * wait up to <i>wait</i> milliseconds for the next available
     * byte array.  if <i>wait</i> equals 0, then wait forever.  <i>wait</i>
     * is advisory and not a hard requirement.  this call should never
     * block indefinitely as in a case where it's backed by a linked-
     * blocking finderQueue and starved for input. if <i>wait</i> is less than 1
     * then the method <i>should</i> act like a poll and return instantly
     * on no data.  this may not be possible in cases where it's backed
     * by blocking file-based inputs.
     *
     * @param wait
     * @return byte[] array or null if no bytes are available within the timeout period
     */
    public byte[] nextBytes(long wait);

    /**
     * @return true if no more bytes will ever be available to nextBytes(), false otherwise
     */
    public boolean isEOF();

    /**
     * close input.  no-op if isEOF() is true.
     */
    public void close();
}
