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

import static io.streamnative.pulsar.handlers.kop.quota.ClientQuotaConstants.QUOTA_CONNECTION_CREATION_RATE;
import static io.streamnative.pulsar.handlers.kop.quota.ClientQuotaConstants.QUOTA_CONSUMER_BYTE_RATE;
import static io.streamnative.pulsar.handlers.kop.quota.ClientQuotaConstants.QUOTA_PRODUCER_BYTE_RATE;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.quota.ClientQuotaIndex.DescribeComponent;
import io.streamnative.pulsar.handlers.kop.quota.ClientQuotaIndex.ResolvedQuota;
import java.util.ArrayList;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

@Slf4j
public class ClientQuotaService implements AutoCloseable {

    @SuppressFBWarnings("EQ_UNUSUAL")
    public record QuotaOp(String key, Double value, boolean remove) {}

    @SuppressFBWarnings("EQ_UNUSUAL")
    public record AlterEntry(List<EntityComponent> entity, List<QuotaOp> ops) {}

    @SuppressFBWarnings("EQ_UNUSUAL")
    public record ThrottleResult(String quotaKey,
                                 long bytes,
                                 long rawThrottleMs,
                                 int throttleTimeMsInResponse,
                                 int muteMs,
                                 ResolvedQuota resolvedQuota) {}

    @SuppressFBWarnings("EQ_UNUSUAL")
    private record LimiterKey(String quotaKey, String user, String clientId) {}

    private final KafkaServiceConfiguration kafkaConfig;
    private final String tenant;
    private final ClientQuotaSnapshotStore snapshotStore;
    private final ClientQuotaStats stats;

    private volatile ClientQuotaIndex index = ClientQuotaIndex.empty();
    private final AtomicLong snapshotLoadSequence = new AtomicLong(0);
    private final AtomicLong lastAppliedMetadataVersion =
            new AtomicLong(ClientQuotaSnapshotStore.NOT_EXISTS_VERSION);

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
            snapshotLoadSequence.incrementAndGet();
            applySnapshotIfNewerOrReset(loaded.snapshot(), loaded.metadataVersion());
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
        Map<ClientQuotaEntityUtils.CanonicalEntityKey, ClientQuotaEntry> current = new HashMap<>();
        if (snapshot != null && snapshot.getEntries() != null) {
            for (ClientQuotaEntry entry : snapshot.getEntries()) {
                List<EntityComponent> entity = ClientQuotaEntityUtils.canonicalize(entry.getEntity());
                ClientQuotaEntityUtils.CanonicalEntityKey entityKey =
                        ClientQuotaEntityUtils.canonicalKeyOfCanonicalEntity(entity);
                Map<String, Double> quotas = canonicalizeQuotas(entry.getQuotas());
                current.putIfAbsent(entityKey, new ClientQuotaEntry(entity, quotas));
            }
        }

        for (AlterEntry alteration : alterations) {
            List<EntityComponent> entity = ClientQuotaEntityUtils.canonicalize(alteration.entity());
            ClientQuotaEntityUtils.CanonicalEntityKey entityKey =
                    ClientQuotaEntityUtils.canonicalKeyOfCanonicalEntity(entity);
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

        List<Map.Entry<ClientQuotaEntityUtils.CanonicalEntityKey, ClientQuotaEntry>> entries =
                new ArrayList<>(current.entrySet());
        entries.sort(Map.Entry.comparingByKey(ClientQuotaEntityUtils::compareCanonicalKeys));
        List<ClientQuotaEntry> ordered = new ArrayList<>(entries.size());
        for (Map.Entry<ClientQuotaEntityUtils.CanonicalEntityKey, ClientQuotaEntry> entry : entries) {
            ordered.add(entry.getValue());
        }
        return new ClientQuotaSnapshot(1, ordered);
    }

    private void reloadAsync() {
        long loadSequence = snapshotLoadSequence.incrementAndGet();
        snapshotStore.load().whenComplete((loaded, ex) -> {
            if (ex != null) {
                stats.recordSnapshotReloadFail();
                log.warn("Failed to reload client quota snapshot on path {} for tenant {}",
                        snapshotStore.snapshotPath(), tenant, ex);
                return;
            }
            if (loadSequence != snapshotLoadSequence.get()) {
                return;
            }
            applySnapshotIfNewerOrReset(loaded.snapshot(), loaded.metadataVersion());
        });
    }

    private void applySnapshotIfNewerOrReset(ClientQuotaSnapshot snapshot, long metadataVersion) {
        if (metadataVersion == ClientQuotaSnapshotStore.NOT_EXISTS_VERSION) {
            // Snapshot path deleted or does not exist => clear quotas.
            lastAppliedMetadataVersion.set(metadataVersion);
            this.index = ClientQuotaIndex.ofSnapshot(snapshot);
            stats.recordSnapshotReloadSuccess(metadataVersion);
            return;
        }
        while (true) {
            long current = lastAppliedMetadataVersion.get();
            if (metadataVersion <= current) {
                return;
            }
            if (lastAppliedMetadataVersion.compareAndSet(current, metadataVersion)) {
                this.index = ClientQuotaIndex.ofSnapshot(snapshot);
                stats.recordSnapshotReloadSuccess(metadataVersion);
                return;
            }
        }
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
