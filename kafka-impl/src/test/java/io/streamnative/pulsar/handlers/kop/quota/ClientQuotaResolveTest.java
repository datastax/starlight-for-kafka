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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.stats.NullStatsLogger;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ClientQuotaResolveTest {

    @Test
    public void testPerKeyFallbackAndResolvedDim() {
        ClientQuotaSnapshot snapshot = new ClientQuotaSnapshot(1, List.of(
                new ClientQuotaEntry(List.of(
                        new EntityComponent("user", "alice"),
                        new EntityComponent("client-id", "c1")
                ), Map.of(ClientQuotaConstants.QUOTA_CONSUMER_BYTE_RATE, 3000.0d)),
                new ClientQuotaEntry(List.of(
                        new EntityComponent("user", "alice"),
                        new EntityComponent("client-id", null)
                ), Map.of(ClientQuotaConstants.QUOTA_PRODUCER_BYTE_RATE, 1000.0d))
        ));
        ClientQuotaIndex index = ClientQuotaIndex.ofSnapshot(snapshot);

        Optional<ClientQuotaIndex.ResolvedQuota> producer = index.resolve("alice", "c1",
                ClientQuotaConstants.QUOTA_PRODUCER_BYTE_RATE);
        assertTrue(producer.isPresent());
        assertEquals(producer.get().resolvedLevel(), 2);
        assertEquals(producer.get().resolvedDim(), ClientQuotaIndex.ResolvedDim.USER);

        Optional<ClientQuotaIndex.ResolvedQuota> consumer = index.resolve("alice", "c1",
                ClientQuotaConstants.QUOTA_CONSUMER_BYTE_RATE);
        assertTrue(consumer.isPresent());
        assertEquals(consumer.get().resolvedLevel(), 1);
        assertEquals(consumer.get().resolvedDim(), ClientQuotaIndex.ResolvedDim.USER_CLIENT);
    }

    @Test
    public void testPrecedenceLevels1To8() {
        String user = "alice";
        String clientId = "c1";
        String quotaKey = ClientQuotaConstants.QUOTA_PRODUCER_BYTE_RATE;

        List<ClientQuotaEntry> entries = List.of(
                new ClientQuotaEntry(List.of(
                        new EntityComponent("user", user),
                        new EntityComponent("client-id", clientId)
                ), Map.of(quotaKey, 1.0d)),
                new ClientQuotaEntry(List.of(
                        new EntityComponent("user", user),
                        new EntityComponent("client-id", null)
                ), Map.of(quotaKey, 2.0d)),
                new ClientQuotaEntry(List.of(
                        new EntityComponent("user", user)
                ), Map.of(quotaKey, 3.0d)),
                new ClientQuotaEntry(List.of(
                        new EntityComponent("user", null),
                        new EntityComponent("client-id", clientId)
                ), Map.of(quotaKey, 4.0d)),
                new ClientQuotaEntry(List.of(
                        new EntityComponent("user", null),
                        new EntityComponent("client-id", null)
                ), Map.of(quotaKey, 5.0d)),
                new ClientQuotaEntry(List.of(
                        new EntityComponent("user", null)
                ), Map.of(quotaKey, 6.0d)),
                new ClientQuotaEntry(List.of(
                        new EntityComponent("client-id", clientId)
                ), Map.of(quotaKey, 7.0d)),
                new ClientQuotaEntry(List.of(
                        new EntityComponent("client-id", null)
                ), Map.of(quotaKey, 8.0d))
        );

        for (int level = 1; level <= 8; level++) {
            ClientQuotaIndex index = ClientQuotaIndex.ofSnapshot(new ClientQuotaSnapshot(1,
                    entries.subList(level - 1, entries.size())));
            Optional<ClientQuotaIndex.ResolvedQuota> resolved = index.resolve(user, clientId, quotaKey);
            assertTrue(resolved.isPresent());
            assertEquals(resolved.get().resolvedLevel(), level);
            assertEquals(resolved.get().quotaValue(), (double) level);
            assertEquals(resolved.get().resolvedDim(), expectedDim(level));
        }
    }

    @Test
    public void testLimiterKeyCollapseForUserLevelQuota() throws Exception {
        KafkaServiceConfiguration config = new KafkaServiceConfiguration();
        config.setKopClientQuotaEnabled(true);
        config.setKopClientQuotaWindowNum(11);
        config.setKopClientQuotaWindowSizeSeconds(1);
        config.setKopClientQuotaLimiterCleanupIntervalMs(0);

        MetadataStoreExtended metadataStore = Mockito.mock(MetadataStoreExtended.class);
        Mockito.when(metadataStore.get(Mockito.anyString()))
                .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(Optional.empty()));

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        ClientQuotaStats stats = new ClientQuotaStats(NullStatsLogger.INSTANCE);
        ClientQuotaService service = new ClientQuotaService(config, "test-cluster", "test-tenant",
                metadataStore, executor, stats);

        ClientQuotaSnapshot snapshot = new ClientQuotaSnapshot(1, List.of(
                new ClientQuotaEntry(List.of(
                        new EntityComponent("user", "alice"),
                        new EntityComponent("client-id", null)
                ), Map.of(ClientQuotaConstants.QUOTA_PRODUCER_BYTE_RATE, 1.0d))
        ));

        Field indexField = ClientQuotaService.class.getDeclaredField("index");
        indexField.setAccessible(true);
        indexField.set(service, ClientQuotaIndex.ofSnapshot(snapshot));

        // Different clientIds should collapse to the same limiter key when resolved on USER level (level 2/3).
        service.enforceBytesQuota("produce", ClientQuotaConstants.QUOTA_PRODUCER_BYTE_RATE,
                "alice", "c1", 1024, System.currentTimeMillis(), false);
        service.enforceBytesQuota("produce", ClientQuotaConstants.QUOTA_PRODUCER_BYTE_RATE,
                "alice", "c2", 1024, System.currentTimeMillis(), false);
        assertEquals(service.limiterCount(), 1);

        service.close();
        stats.close();
        executor.shutdownNow();
    }

    private static ClientQuotaIndex.ResolvedDim expectedDim(int level) {
        return switch (level) {
            case 1 -> ClientQuotaIndex.ResolvedDim.USER_CLIENT;
            case 2, 3 -> ClientQuotaIndex.ResolvedDim.USER;
            case 4, 7 -> ClientQuotaIndex.ResolvedDim.CLIENT;
            case 5, 6, 8 -> ClientQuotaIndex.ResolvedDim.NONE;
            default -> ClientQuotaIndex.ResolvedDim.NONE;
        };
    }
}
