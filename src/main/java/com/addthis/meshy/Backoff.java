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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Backoff utility.
 * <p/>
 * Two values may be specified upon creation to configure the object: {@code unitDelay} and {@code maxDelayUnit}.
 * The actual backoff delay are calculated based on those values and the backoff counter using this formula:
 * <p/>
 * {@code delay = unitDelay * 2 ^ (max(counter, maxDelayUnit) - 1)}
 * <p/>
 * This is best illustrated with an example. Given unitDelay=1000 and maxDelayUnit=4, the backoff delays are:
 * <pre>
 * counter    delay
 * ----------------
 * 0          0
 * 1          1000
 * 2          2000
 * 3          4000
 * >=4        8000
 * </pre>
 */
public class Backoff {

    public final int unitDelay;
    public final int maxDelayUnit;

    private AtomicInteger counter = new AtomicInteger(0);

    /**
     * Constructor.
     *
     * @param unitDelay
     * @param maxDelayUnit
     */
    public Backoff(int unitDelay, int maxDelayUnit) {
        this.unitDelay = unitDelay;
        this.maxDelayUnit = maxDelayUnit;
    }

    /**
     * Increments the backoff counter.
     *
     * @return the new value
     */
    public int inc() {
        return counter.incrementAndGet();
    }

    /**
     * Resets the backoff counter.
     *
     * @return the old value
     */
    public int reset() {
        return counter.getAndSet(0);
    }

    /**
     * Calculates the delay based on the current backoff counter value.
     *
     * @return delay in millis.
     */
    public int calcDelay() {
        int c = Math.min(counter.get(), maxDelayUnit);
        if (c > 0) {
            return unitDelay << (c - 1);
        } else {
            return 0;
        }
    }

    /**
     * Pauses the current thread for a period of time as calculated by {@link #calcDelay()}.
     */
    public void backoff() {
        int delay = calcDelay();
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                // Do nothing
            }
        }
    }
}

