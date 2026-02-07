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

import io.streamnative.pulsar.handlers.kop.quota.ClientQuotaIndex.ResolvedQuota;
import io.streamnative.pulsar.handlers.kop.stats.StatsLogger;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;

public class ClientQuotaStats implements AutoCloseable {

    private static final String SCOPE = "kop_quota";

    private static final String METRIC_THROTTLE_MS = "kop_quota_throttle_ms";
    private static final String METRIC_THROTTLED_REQUESTS_TOTAL = "kop_quota_throttled_requests_total";
    private static final String METRIC_LIMITERS_TOTAL = "kop_quota_limiters_total";
    private static final String METRIC_LIMITERS_EVICTED_TOTAL = "kop_quota_limiters_evicted_total";
    private static final String METRIC_SNAPSHOT_RELOAD_TOTAL = "kop_quota_snapshot_reload_total";
    private static final String METRIC_SNAPSHOT_VERSION = "kop_quota_snapshot_version";

    private final StatsLogger root;

    private final Counter snapshotReloadSuccess;
    private final Counter snapshotReloadFail;
    private final AtomicLong snapshotVersion = new AtomicLong(-1);

    private final Counter limitersEvictedTotal;
    private final AtomicInteger limitersTotal = new AtomicInteger(0);

    private final Gauge<Number> snapshotVersionGauge = new Gauge<Number>() {
        @Override
        public Number getDefaultValue() {
            return -1;
        }

        @Override
        public Number getSample() {
            return snapshotVersion;
        }
    };

    private final Gauge<Number> limitersTotalGauge = new Gauge<Number>() {
        @Override
        public Number getDefaultValue() {
            return 0;
        }

        @Override
        public Number getSample() {
            return limitersTotal;
        }
    };

    private final ConcurrentHashMap<String, OpStatsLogger> throttleMsLoggers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> throttledRequestsCounters = new ConcurrentHashMap<>();

    public ClientQuotaStats(StatsLogger statsLogger) {
        this.root = Objects.requireNonNull(statsLogger, "statsLogger").scope(SCOPE);
        this.snapshotReloadSuccess = root.scopeLabel("result", "success").getCounter(METRIC_SNAPSHOT_RELOAD_TOTAL);
        this.snapshotReloadFail = root.scopeLabel("result", "fail").getCounter(METRIC_SNAPSHOT_RELOAD_TOTAL);
        this.limitersEvictedTotal = root.getCounter(METRIC_LIMITERS_EVICTED_TOTAL);
        root.registerGauge(METRIC_SNAPSHOT_VERSION, snapshotVersionGauge);
        root.registerGauge(METRIC_LIMITERS_TOTAL, limitersTotalGauge);
    }

    public void recordSnapshotReloadSuccess(long metadataVersion) {
        snapshotReloadSuccess.inc();
        snapshotVersion.set(metadataVersion);
    }

    public void recordSnapshotReloadFail() {
        snapshotReloadFail.inc();
    }

    public void setLimitersTotal(int total) {
        limitersTotal.set(total);
    }

    public void recordLimitersEvicted(int count) {
        for (int i = 0; i < count; i++) {
            limitersEvictedTotal.inc();
        }
    }

    public void recordThrottle(String api, ResolvedQuota resolvedQuota, long throttleMs) {
        if (throttleMs <= 0) {
            return;
        }
        String level = Integer.toString(resolvedQuota.resolvedLevel());
        String dim = resolvedQuota.resolvedDim().label();
        OpStatsLogger throttleLogger = throttleMsLoggers.computeIfAbsent(
                api + "|" + dim + "|" + level,
                __ -> root.scopeLabel("api", api)
                        .scopeLabel("dim", dim)
                        .scopeLabel("level", level)
                        .getOpStatsLogger(METRIC_THROTTLE_MS));
        throttleLogger.registerSuccessfulEvent(throttleMs, TimeUnit.MILLISECONDS);

        Counter counter = throttledRequestsCounters.computeIfAbsent(
                api + "|" + dim + "|" + level,
                __ -> root.scopeLabel("api", api)
                        .scopeLabel("dim", dim)
                        .scopeLabel("level", level)
                        .getCounter(METRIC_THROTTLED_REQUESTS_TOTAL));
        counter.inc();
    }

    @Override
    public void close() {
        root.unregisterGauge(METRIC_SNAPSHOT_VERSION, snapshotVersionGauge);
        root.unregisterGauge(METRIC_LIMITERS_TOTAL, limitersTotalGauge);
    }
}
