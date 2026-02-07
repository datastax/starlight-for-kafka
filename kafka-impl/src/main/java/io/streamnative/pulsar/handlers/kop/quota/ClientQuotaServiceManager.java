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

import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.stats.StatsLogger;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

public class ClientQuotaServiceManager implements AutoCloseable {

    private final KafkaServiceConfiguration kafkaConfig;
    private final String clusterName;
    private final MetadataStoreExtended metadataStore;
    private final ScheduledExecutorService executor;
    private final ClientQuotaStats stats;
    private final ConcurrentHashMap<String, ClientQuotaService> servicesByTenant = new ConcurrentHashMap<>();
    private final ScheduledFuture<?> limiterGaugeRefreshFuture;

    public ClientQuotaServiceManager(KafkaServiceConfiguration kafkaConfig,
                                     String clusterName,
                                     MetadataStoreExtended metadataStore,
                                     ScheduledExecutorService executor,
                                     StatsLogger statsLogger) {
        this.kafkaConfig = Objects.requireNonNull(kafkaConfig, "kafkaConfig");
        this.clusterName = Objects.requireNonNull(clusterName, "clusterName");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.stats = new ClientQuotaStats(statsLogger);

        long intervalMs = kafkaConfig.getKopClientQuotaLimiterCleanupIntervalMs();
        if (intervalMs > 0) {
            this.limiterGaugeRefreshFuture = executor.scheduleWithFixedDelay(
                    this::refreshLimitersTotal,
                    intervalMs,
                    intervalMs,
                    TimeUnit.MILLISECONDS);
        } else {
            this.limiterGaugeRefreshFuture = null;
        }
    }

    public ClientQuotaService getOrCreate(String tenant) {
        return servicesByTenant.computeIfAbsent(Objects.requireNonNull(tenant, "tenant"),
                t -> new ClientQuotaService(kafkaConfig, clusterName, t, metadataStore, executor, stats));
    }

    private void refreshLimitersTotal() {
        int total = 0;
        for (ClientQuotaService service : servicesByTenant.values()) {
            total += service.limiterCount();
        }
        stats.setLimitersTotal(total);
    }

    @Override
    public void close() {
        if (limiterGaugeRefreshFuture != null) {
            limiterGaugeRefreshFuture.cancel(false);
        }
        servicesByTenant.values().forEach(ClientQuotaService::close);
        stats.close();
    }
}

