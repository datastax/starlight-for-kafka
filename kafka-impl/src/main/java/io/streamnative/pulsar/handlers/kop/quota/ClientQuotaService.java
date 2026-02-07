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

import static io.streamnative.pulsar.handlers.kop.quota.ClientQuotaConstants.ENTITY_TYPE_CANONICAL_ORDER;
import static io.streamnative.pulsar.handlers.kop.quota.ClientQuotaConstants.QUOTA_CONNECTION_CREATION_RATE;
import static io.streamnative.pulsar.handlers.kop.quota.ClientQuotaConstants.QUOTA_CONSUMER_BYTE_RATE;
import static io.streamnative.pulsar.handlers.kop.quota.ClientQuotaConstants.QUOTA_PRODUCER_BYTE_RATE;

import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.quota.ClientQuotaIndex.DescribeComponent;
import io.streamnative.pulsar.handlers.kop.quota.ClientQuotaIndex.ResolvedQuota;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

@Slf4j
public class ClientQuotaService implements AutoCloseable {

    public record QuotaOp(String key, Double value, boolean remove) {}

    public record AlterEntry(List<EntityComponent> entity, List<QuotaOp> ops) {}

    public record ThrottleResult(String quotaKey,
                                 long bytes,
                                 long rawThrottleMs,
                                 int throttleTimeMsInResponse,
                                 int muteMs,
                                 ResolvedQuota resolvedQuota) {}

    private record LimiterKey(String quotaKey, String user, String clientId) {}

    private final KafkaServiceConfiguration kafkaConfig;
    private final String tenant;
    private final ClientQuotaSnapshotStore snapshotStore;
    private final ClientQuotaStats stats;

    private volatile ClientQuotaIndex index = ClientQuotaIndex.empty();

    private final ConcurrentHashMap<LimiterKey, SlidingWindowLimiter> limiters = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executor;
    private final ScheduledFuture<?> limiterCleanupFuture;

    public ClientQuotaService(KafkaServiceConfiguration kafkaConfig,
                              String clusterName,
                              String tenant,
                              MetadataStoreExtended metadataStore,
                              ScheduledExecutorService executor,
                              ClientQuotaStats stats) {
        this.kafkaConfig = Objects.requireNonNull(kafkaConfig, "kafkaConfig");
        this.tenant = Objects.requireNonNull(tenant, "tenant");
        this.stats = Objects.requireNonNull(stats, "stats");
        this.executor = Objects.requireNonNull(executor, "executor");

        String path = ClientQuotaSnapshotStore.buildSnapshotPath(clusterName, tenant);
        this.snapshotStore = new ClientQuotaSnapshotStore(metadataStore, path);
        this.snapshotStore.registerListener(this::reloadAsync);
        reloadAsync();

        long cleanupIntervalMs = kafkaConfig.getKopClientQuotaLimiterCleanupIntervalMs();
        if (cleanupIntervalMs > 0) {
            this.limiterCleanupFuture = executor.scheduleWithFixedDelay(
                    this::cleanupLimiters,
                    cleanupIntervalMs,
                    cleanupIntervalMs,
                    TimeUnit.MILLISECONDS);
        } else {
            this.limiterCleanupFuture = null;
        }
    }

    public String tenant() {
        return tenant;
    }

    public List<ClientQuotaEntry> describe(List<DescribeComponent> filters, boolean strict) {
        return index.describe(filters, strict);
    }

    public Optional<ResolvedQuota> resolve(String user, String clientId, String quotaKey) {
        return index.resolve(user, clientId, quotaKey);
    }

    public CompletableFuture<Long> alter(List<AlterEntry> alterations) {
        if (alterations == null || alterations.isEmpty()) {
            return CompletableFuture.completedFuture(-1L);
        }
        UnaryOperator<ClientQuotaSnapshot> mutator = snapshot -> applyAlterations(snapshot, alterations);
        return snapshotStore.updateWithCas(mutator).thenApply(loaded -> {
            this.index = ClientQuotaIndex.ofSnapshot(loaded.snapshot());
            stats.recordSnapshotReloadSuccess(loaded.metadataVersion());
            return loaded.metadataVersion();
        });
    }

    public Optional<ThrottleResult> enforceBytesQuota(String api,
                                                      String quotaKey,
                                                      String user,
                                                      String clientId,
                                                      long bytes,
                                                      long nowMs,
                                                      boolean unrecordIfThrottled) {
        if (!kafkaConfig.isKopClientQuotaEnabled()) {
            return Optional.empty();
        }
        if (bytes <= 0) {
            return Optional.empty();
        }
        Optional<ResolvedQuota> resolvedOpt = index.resolve(user, clientId, quotaKey);
        if (resolvedOpt.isEmpty()) {
            return Optional.empty();
        }
        ResolvedQuota resolved = resolvedOpt.get();

        LimiterKey limiterKey = limiterKey(quotaKey, resolved.resolvedDim(), user, clientId);
        SlidingWindowLimiter limiter = limiters.computeIfAbsent(
                limiterKey,
                __ -> new SlidingWindowLimiter(
                        kafkaConfig.getKopClientQuotaWindowNum(),
                        TimeUnit.SECONDS.toMillis(kafkaConfig.getKopClientQuotaWindowSizeSeconds())));

        limiter.record(bytes, nowMs);
        long rawThrottleMs = limiter.throttleTimeMs(resolved.quotaValue(), nowMs);
        if (rawThrottleMs <= 0) {
            return Optional.empty();
        }
        if (unrecordIfThrottled) {
            limiter.unrecord(bytes, nowMs);
        }

        int throttleTimeMsInResponse = capToInt(Math.min(
                rawThrottleMs,
                Math.min(kafkaConfig.getKopClientQuotaMaxThrottleTimeInResponseMs(),
                        kafkaConfig.getKopClientQuotaHardMaxThrottleTimeMs())));
        int muteMs = capToInt(Math.min(
                rawThrottleMs,
                Math.min(kafkaConfig.getKopClientQuotaMaxMuteTimeMs(),
                        kafkaConfig.getKopClientQuotaHardMaxThrottleTimeMs())));

        stats.recordThrottle(api, resolved, throttleTimeMsInResponse);
        return Optional.of(new ThrottleResult(quotaKey, bytes, rawThrottleMs, throttleTimeMsInResponse, muteMs,
                resolved));
    }

    public int limiterCount() {
        return limiters.size();
    }

    private static int capToInt(long value) {
        if (value <= 0) {
            return 0;
        }
        if (value > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) value;
    }

    private static LimiterKey limiterKey(String quotaKey, ClientQuotaIndex.ResolvedDim dim, String user,
                                         String clientId) {
        return switch (dim) {
            case USER_CLIENT -> new LimiterKey(quotaKey, user, clientId);
            case USER -> new LimiterKey(quotaKey, user, null);
            case CLIENT -> new LimiterKey(quotaKey, null, clientId);
            case NONE -> new LimiterKey(quotaKey, null, null);
        };
    }

    private ClientQuotaSnapshot applyAlterations(ClientQuotaSnapshot snapshot, List<AlterEntry> alterations) {
        Map<String, ClientQuotaEntry> current = new HashMap<>();
        if (snapshot != null && snapshot.getEntries() != null) {
            for (ClientQuotaEntry entry : snapshot.getEntries()) {
                List<EntityComponent> entity = canonicalizeEntity(entry.getEntity());
                String entityKey = canonicalEntityString(entity);
                Map<String, Double> quotas = canonicalizeQuotas(entry.getQuotas());
                current.putIfAbsent(entityKey, new ClientQuotaEntry(entity, quotas));
            }
        }

        for (AlterEntry alteration : alterations) {
            List<EntityComponent> entity = canonicalizeEntity(alteration.entity());
            String entityKey = canonicalEntityString(entity);
            ClientQuotaEntry existing = current.computeIfAbsent(entityKey,
                    __ -> new ClientQuotaEntry(entity, new TreeMap<>()));
            Map<String, Double> quotas = existing.getQuotas();
            if (quotas == null) {
                quotas = new TreeMap<>();
                existing.setQuotas(quotas);
            }

            for (QuotaOp op : alteration.ops()) {
                if (op.remove()) {
                    quotas.remove(op.key());
                } else {
                    quotas.put(op.key(), canonicalizeQuotaValue(op.key(), op.value()));
                }
            }

            if (quotas.isEmpty()) {
                current.remove(entityKey);
            } else {
                existing.setEntity(entity);
                existing.setQuotas(canonicalizeQuotas(quotas));
            }
        }

        List<ClientQuotaEntry> entries = new ArrayList<>(current.values());
        entries.sort(Comparator.comparing(e -> canonicalEntityString(e.getEntity())));
        return new ClientQuotaSnapshot(1, entries);
    }

    private void reloadAsync() {
        snapshotStore.load().whenComplete((loaded, ex) -> {
            if (ex != null) {
                stats.recordSnapshotReloadFail();
                log.warn("Failed to reload client quota snapshot on path {} for tenant {}",
                        snapshotStore.snapshotPath(), tenant, ex);
                return;
            }
            this.index = ClientQuotaIndex.ofSnapshot(loaded.snapshot());
            stats.recordSnapshotReloadSuccess(loaded.metadataVersion());
        });
    }

    private void cleanupLimiters() {
        long expireMs = kafkaConfig.getKopClientQuotaLimiterExpireMs();
        if (expireMs <= 0) {
            return;
        }
        long nowMs = System.currentTimeMillis();
        int evicted = 0;
        for (Map.Entry<LimiterKey, SlidingWindowLimiter> entry : limiters.entrySet()) {
            SlidingWindowLimiter limiter = entry.getValue();
            if (nowMs - limiter.lastAccessTimeMs() > expireMs) {
                if (limiters.remove(entry.getKey(), limiter)) {
                    evicted++;
                }
            }
        }
        if (evicted > 0) {
            stats.recordLimitersEvicted(evicted);
        }
    }

    private static List<EntityComponent> canonicalizeEntity(List<EntityComponent> entity) {
        if (entity == null || entity.isEmpty()) {
            return Collections.emptyList();
        }
        List<EntityComponent> copy = new ArrayList<>(entity.size());
        for (EntityComponent component : entity) {
            if (component != null) {
                copy.add(component);
            }
        }
        copy.sort(Comparator.comparingInt(c -> typeOrder(c.getType())));
        return copy;
    }

    private static String canonicalEntityString(List<EntityComponent> entity) {
        if (entity == null || entity.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (EntityComponent component : entity) {
            if (component == null) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append('|');
            }
            sb.append(component.getType()).append('=');
            if (component.getName() == null) {
                sb.append("<default>");
            } else {
                sb.append(component.getName());
            }
        }
        return sb.toString();
    }

    private static int typeOrder(String entityType) {
        if (entityType == null) {
            return Integer.MAX_VALUE;
        }
        int idx = ENTITY_TYPE_CANONICAL_ORDER.indexOf(entityType);
        return idx >= 0 ? idx : Integer.MAX_VALUE;
    }

    private static Map<String, Double> canonicalizeQuotas(Map<String, Double> quotas) {
        if (quotas == null || quotas.isEmpty()) {
            return new TreeMap<>();
        }
        Map<String, Double> canonical = new TreeMap<>();
        quotas.forEach((key, value) -> canonical.put(key, canonicalizeQuotaValue(key, value)));
        return canonical;
    }

    private static double canonicalizeQuotaValue(String key, Double value) {
        if (value == null) {
            return 0.0d;
        }
        if (QUOTA_PRODUCER_BYTE_RATE.equals(key)
                || QUOTA_CONSUMER_BYTE_RATE.equals(key)
                || QUOTA_CONNECTION_CREATION_RATE.equals(key)) {
            double rounded = Math.rint(value);
            if (Double.isFinite(value) && Math.abs(value - rounded) < 1e-9d) {
                return rounded;
            }
        }
        return value;
    }

    public static void validateEntityComponentsNoDuplicates(List<EntityComponent> entity) {
        if (entity == null) {
            return;
        }
        Set<String> types = new HashSet<>(entity.size());
        for (EntityComponent c : entity) {
            if (c == null || c.getType() == null) {
                continue;
            }
            if (!types.add(c.getType())) {
                throw new IllegalArgumentException("Duplicate entityType in entity: " + c.getType());
            }
        }
    }

    @Override
    public void close() {
        if (limiterCleanupFuture != null) {
            limiterCleanupFuture.cancel(false);
        }
    }
}
