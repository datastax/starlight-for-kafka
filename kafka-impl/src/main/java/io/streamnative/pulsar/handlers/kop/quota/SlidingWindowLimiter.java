/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.kop.quota;

import java.util.Arrays;

/**
 * A simple sliding-window limiter based on {@code windowNum} fixed-size buckets.
 *
 * <p>It keeps only the aggregate amount for each bucket and returns a throttle time (ms) until the
 * bucket(s) that make the moving sum exceed the quota fall out of the window.</p>
 */
public class SlidingWindowLimiter {

    private final int windowNum;
    private final long windowSizeMs;
    private final long windowDurationMs;

    private final long[] bucketValues;
    private long lastBucketId = -1;
    private long totalValue = 0;
    private volatile long lastAccessTimeMs = 0;

    public SlidingWindowLimiter(int windowNum, long windowSizeMs) {
        if (windowNum <= 0) {
            throw new IllegalArgumentException("windowNum must be positive, but got " + windowNum);
        }
        if (windowSizeMs <= 0) {
            throw new IllegalArgumentException("windowSizeMs must be positive, but got " + windowSizeMs);
        }
        this.windowNum = windowNum;
        this.windowSizeMs = windowSizeMs;
        this.windowDurationMs = Math.multiplyExact(windowNum, windowSizeMs);
        this.bucketValues = new long[windowNum];
    }

    public long lastAccessTimeMs() {
        return lastAccessTimeMs;
    }

    public synchronized void record(long value, long nowMs) {
        rotateTo(nowMs);
        int index = indexForBucket(lastBucketId);
        bucketValues[index] += value;
        totalValue += value;
        lastAccessTimeMs = nowMs;
    }

    public synchronized void unrecord(long value, long nowMs) {
        rotateTo(nowMs);
        int index = indexForBucket(lastBucketId);
        long current = bucketValues[index];
        long delta = Math.min(current, value);
        bucketValues[index] -= delta;
        totalValue -= delta;
        lastAccessTimeMs = nowMs;
    }

    public synchronized long throttleTimeMs(double quotaPerSecond, long nowMs) {
        rotateTo(nowMs);
        lastAccessTimeMs = nowMs;

        double allowedTotal = quotaPerSecond * windowDurationMs / 1000.0d;
        if (totalValue <= allowedTotal) {
            return 0;
        }

        long bucketId = bucketId(nowMs);
        long nextBoundaryMs = (bucketId + 1) * windowSizeMs;
        long waitToNextBoundaryMs = Math.max(0, nextBoundaryMs - nowMs);

        long simulatedTotal = totalValue;
        for (int step = 1; step <= windowNum; step++) {
            int indexToClear = indexForBucket(bucketId + step);
            simulatedTotal -= bucketValues[indexToClear];
            if (simulatedTotal <= allowedTotal) {
                return waitToNextBoundaryMs + (long) (step - 1) * windowSizeMs;
            }
        }
        return waitToNextBoundaryMs + windowDurationMs;
    }

    private void rotateTo(long nowMs) {
        long bucketId = bucketId(nowMs);
        if (lastBucketId == -1) {
            lastBucketId = bucketId;
            lastAccessTimeMs = nowMs;
            return;
        }
        long diff = bucketId - lastBucketId;
        if (diff <= 0) {
            lastAccessTimeMs = nowMs;
            return;
        }

        if (diff >= windowNum) {
            Arrays.fill(bucketValues, 0);
            totalValue = 0;
        } else {
            for (long i = 1; i <= diff; i++) {
                int indexToClear = indexForBucket(lastBucketId + i);
                totalValue -= bucketValues[indexToClear];
                bucketValues[indexToClear] = 0;
            }
        }
        lastBucketId = bucketId;
        lastAccessTimeMs = nowMs;
    }

    private long bucketId(long nowMs) {
        return nowMs / windowSizeMs;
    }

    private int indexForBucket(long bucketId) {
        return (int) Math.floorMod(bucketId, windowNum);
    }
}

