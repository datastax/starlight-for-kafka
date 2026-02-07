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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

@Slf4j
public class ClientQuotaSnapshotStore {

    public static final long NOT_EXISTS_VERSION = -1L;
    private static final int SCHEMA_VERSION = 1;

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);

    public record LoadedSnapshot(ClientQuotaSnapshot snapshot, long metadataVersion) {}

    private final MetadataStoreExtended metadataStore;
    private final String snapshotPath;

    public ClientQuotaSnapshotStore(MetadataStoreExtended metadataStore, String snapshotPath) {
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.snapshotPath = Objects.requireNonNull(snapshotPath, "snapshotPath");
    }

    public String snapshotPath() {
        return snapshotPath;
    }

    public static String buildSnapshotPath(String clusterName, String tenant) {
        String encodedCluster = urlEncodePathSegment(Objects.requireNonNull(clusterName, "clusterName"));
        String encodedTenant = urlEncodePathSegment(Objects.requireNonNull(tenant, "tenant"));
        return "/kop/client_quotas/" + encodedCluster + "/" + encodedTenant + "/snapshot";
    }

    private static String urlEncodePathSegment(String raw) {
        return URLEncoder.encode(raw, StandardCharsets.UTF_8);
    }

    public void registerListener(Runnable onChanged) {
        Objects.requireNonNull(onChanged, "onChanged");
        metadataStore.registerListener(notification -> handleNotification(notification, onChanged));
        // establish watch
        metadataStore.get(snapshotPath);
    }

    public CompletableFuture<LoadedSnapshot> load() {
        return metadataStore.get(snapshotPath).thenApply(resultOpt -> {
            if (resultOpt.isEmpty()) {
                return new LoadedSnapshot(emptySnapshot(), NOT_EXISTS_VERSION);
            }
            GetResult result = resultOpt.get();
            byte[] bytes = result.getValue();
            if (bytes == null || bytes.length == 0) {
                return new LoadedSnapshot(emptySnapshot(), result.getStat().getVersion());
            }
            try {
                ClientQuotaSnapshot snapshot = MAPPER.readValue(bytes, ClientQuotaSnapshot.class);
                if (snapshot.getVersion() != SCHEMA_VERSION) {
                    log.warn("Unexpected client quota snapshot schema version {} on path {}",
                            snapshot.getVersion(), snapshotPath);
                }
                return new LoadedSnapshot(snapshot, result.getStat().getVersion());
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse client quota snapshot on path " + snapshotPath, e);
            }
        });
    }

    public CompletableFuture<LoadedSnapshot> updateWithCas(UnaryOperator<ClientQuotaSnapshot> mutator) {
        Objects.requireNonNull(mutator, "mutator");
        CompletableFuture<LoadedSnapshot> future = new CompletableFuture<>();
        updateWithCasInternal(mutator, future, 0);
        return future;
    }

    private void updateWithCasInternal(UnaryOperator<ClientQuotaSnapshot> mutator,
                                       CompletableFuture<LoadedSnapshot> future,
                                       int attempt) {
        if (attempt > 10) {
            future.completeExceptionally(new RuntimeException("Exceeded CAS retry limit updating " + snapshotPath));
            return;
        }

        load().whenComplete((loaded, loadEx) -> {
            if (loadEx != null) {
                future.completeExceptionally(loadEx);
                return;
            }
            ClientQuotaSnapshot mutated = mutator.apply(loaded.snapshot);
            byte[] bytes;
            try {
                bytes = MAPPER.writeValueAsBytes(mutated);
            } catch (JsonProcessingException e) {
                future.completeExceptionally(e);
                return;
            }

            metadataStore.put(snapshotPath, bytes, Optional.of(loaded.metadataVersion))
                    .thenAccept(stat -> future.complete(new LoadedSnapshot(mutated, stat.getVersion())))
                    .exceptionally(ex -> {
                        Throwable cause = ex.getCause() == null ? ex : ex.getCause();
                        if (cause instanceof MetadataStoreException.BadVersionException) {
                            updateWithCasInternal(mutator, future, attempt + 1);
                            return null;
                        }
                        future.completeExceptionally(cause);
                        return null;
                    });
        });
    }

    private void handleNotification(Notification notification, Runnable onChanged) {
        if (notification == null || notification.getPath() == null) {
            return;
        }
        if (!snapshotPath.equals(notification.getPath())) {
            return;
        }
        onChanged.run();
        // re-establish watch
        metadataStore.get(snapshotPath);
    }

    private static ClientQuotaSnapshot emptySnapshot() {
        return new ClientQuotaSnapshot(SCHEMA_VERSION, java.util.Collections.emptyList());
    }
}

