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

import static io.streamnative.pulsar.handlers.kop.quota.ClientQuotaConstants.ENTITY_TYPE_CLIENT_ID;
import static io.streamnative.pulsar.handlers.kop.quota.ClientQuotaConstants.ENTITY_TYPE_IP;
import static io.streamnative.pulsar.handlers.kop.quota.ClientQuotaConstants.ENTITY_TYPE_USER;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class ClientQuotaIndex {

    public enum ResolvedDim {
        USER_CLIENT("user_client"),
        USER("user"),
        CLIENT("client"),
        NONE("none");

        private final String label;

        ResolvedDim(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }

    @SuppressFBWarnings("EQ_UNUSUAL")
    public record ResolvedQuota(String quotaKey, double quotaValue, int resolvedLevel, ResolvedDim resolvedDim) {}

    @SuppressFBWarnings("EQ_UNUSUAL")
    public record DescribeComponent(String entityType, byte matchType, String match) {}

    private static final class IndexedEntry {
        private final EntityKey key;
        private final ClientQuotaEntry entry;

        private IndexedEntry(EntityKey key, ClientQuotaEntry entry) {
            this.key = key;
            this.entry = entry;
        }
    }

    private static final class EntityKey {
        private final boolean hasUser;
        private final String user;
        private final boolean hasClientId;
        private final String clientId;
        private final boolean hasIp;
        private final String ip;

        private EntityKey(boolean hasUser, String user, boolean hasClientId,
                          String clientId, boolean hasIp, String ip) {
            this.hasUser = hasUser;
            this.user = user;
            this.hasClientId = hasClientId;
            this.clientId = clientId;
            this.hasIp = hasIp;
            this.ip = hasIp ? ip : null;
        }

        static EntityKey ofUserClient(String user, String clientId) {
            return new EntityKey(true, user, true, clientId, false, null);
        }

        static EntityKey ofUserOnly(String user) {
            return new EntityKey(true, user, false, null, false, null);
        }

        static EntityKey ofClientOnly(String clientId) {
            return new EntityKey(false, null, true, clientId, false, null);
        }

        boolean hasType(String entityType) {
            return switch (entityType) {
                case ENTITY_TYPE_USER -> hasUser;
                case ENTITY_TYPE_CLIENT_ID -> hasClientId;
                case ENTITY_TYPE_IP -> hasIp;
                default -> false;
            };
        }

        String nameOf(String entityType) {
            return switch (entityType) {
                case ENTITY_TYPE_USER -> user;
                case ENTITY_TYPE_CLIENT_ID -> clientId;
                case ENTITY_TYPE_IP -> ip;
                default -> null;
            };
        }

        Set<String> typeSet() {
            Set<String> set = new HashSet<>(3);
            if (hasUser) {
                set.add(ENTITY_TYPE_USER);
            }
            if (hasClientId) {
                set.add(ENTITY_TYPE_CLIENT_ID);
            }
            if (hasIp) {
                set.add(ENTITY_TYPE_IP);
            }
            return set;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            EntityKey entityKey = (EntityKey) o;
            return hasUser == entityKey.hasUser
                    && Objects.equals(user, entityKey.user)
                    && hasClientId == entityKey.hasClientId
                    && Objects.equals(clientId, entityKey.clientId)
                    && hasIp == entityKey.hasIp
                    && Objects.equals(ip, entityKey.ip);
        }

        @Override
        public int hashCode() {
            return Objects.hash(hasUser, user, hasClientId, clientId, hasIp, ip);
        }
    }

    private final List<IndexedEntry> entries;
    private final Map<EntityKey, Map<String, Double>> quotasByEntity;

    private ClientQuotaIndex(List<IndexedEntry> entries, Map<EntityKey, Map<String, Double>> quotasByEntity) {
        this.entries = entries;
        this.quotasByEntity = quotasByEntity;
    }

    public static ClientQuotaIndex empty() {
        return new ClientQuotaIndex(Collections.emptyList(), Collections.emptyMap());
    }

    public static ClientQuotaIndex ofSnapshot(ClientQuotaSnapshot snapshot) {
        if (snapshot == null || snapshot.getEntries() == null || snapshot.getEntries().isEmpty()) {
            return empty();
        }
        Map<EntityKey, Map<String, Double>> quotasByEntity = new HashMap<>(snapshot.getEntries().size());
        List<IndexedEntry> entries = new ArrayList<>(snapshot.getEntries().size());
        for (ClientQuotaEntry entry : snapshot.getEntries()) {
            EntityKey key = toEntityKey(entry);
            Map<String, Double> quotas = entry.getQuotas() == null ? Collections.emptyMap() : entry.getQuotas();
            quotasByEntity.putIfAbsent(key, Collections.unmodifiableMap(new HashMap<>(quotas)));
            entries.add(new IndexedEntry(key, entry));
        }
        return new ClientQuotaIndex(Collections.unmodifiableList(entries), Collections.unmodifiableMap(quotasByEntity));
    }

    public List<ClientQuotaEntry> entries() {
        if (entries.isEmpty()) {
            return Collections.emptyList();
        }
        List<ClientQuotaEntry> result = new ArrayList<>(entries.size());
        for (IndexedEntry entry : entries) {
            result.add(entry.entry);
        }
        return result;
    }

    public List<ClientQuotaEntry> describe(List<DescribeComponent> filters, boolean strict) {
        if (entries.isEmpty()) {
            return Collections.emptyList();
        }
        List<ClientQuotaEntry> result = new ArrayList<>();
        for (IndexedEntry indexed : entries) {
            if (matchesDescribeFilters(indexed.key, filters, strict)) {
                result.add(indexed.entry);
            }
        }
        return result;
    }

    public Optional<ResolvedQuota> resolve(String user, String clientId, String quotaKey) {
        if (quotaKey == null) {
            return Optional.empty();
        }
        // Kafka precedence (1..8), resolved per quota key.
        // 1. {user=U, client-id=C}
        Optional<ResolvedQuota> resolved = resolveAtLevel(1, EntityKey.ofUserClient(user, clientId), quotaKey);
        if (resolved.isPresent()) {
            return resolved;
        }
        // 2. {user=U, client-id=null}
        resolved = resolveAtLevel(2, EntityKey.ofUserClient(user, null), quotaKey);
        if (resolved.isPresent()) {
            return resolved;
        }
        // 3. {user=U}
        resolved = resolveAtLevel(3, EntityKey.ofUserOnly(user), quotaKey);
        if (resolved.isPresent()) {
            return resolved;
        }
        // 4. {user=null, client-id=C}
        resolved = resolveAtLevel(4, EntityKey.ofUserClient(null, clientId), quotaKey);
        if (resolved.isPresent()) {
            return resolved;
        }
        // 5. {user=null, client-id=null}
        resolved = resolveAtLevel(5, EntityKey.ofUserClient(null, null), quotaKey);
        if (resolved.isPresent()) {
            return resolved;
        }
        // 6. {user=null}
        resolved = resolveAtLevel(6, EntityKey.ofUserOnly(null), quotaKey);
        if (resolved.isPresent()) {
            return resolved;
        }
        // 7. {client-id=C}
        resolved = resolveAtLevel(7, EntityKey.ofClientOnly(clientId), quotaKey);
        if (resolved.isPresent()) {
            return resolved;
        }
        // 8. {client-id=null}
        return resolveAtLevel(8, EntityKey.ofClientOnly(null), quotaKey);
    }

    private Optional<ResolvedQuota> resolveAtLevel(int level, EntityKey key, String quotaKey) {
        Map<String, Double> quotas = quotasByEntity.get(key);
        if (quotas == null) {
            return Optional.empty();
        }
        Double value = quotas.get(quotaKey);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(new ResolvedQuota(quotaKey, value, level, dimOfLevel(level)));
    }

    private ResolvedDim dimOfLevel(int level) {
        return switch (level) {
            case 1 -> ResolvedDim.USER_CLIENT;
            case 2, 3 -> ResolvedDim.USER;
            case 4, 7 -> ResolvedDim.CLIENT;
            case 5, 6, 8 -> ResolvedDim.NONE;
            default -> ResolvedDim.NONE;
        };
    }

    private boolean matchesDescribeFilters(EntityKey entityKey, List<DescribeComponent> filters, boolean strict) {
        if (filters == null) {
            filters = Collections.emptyList();
        }
        if (strict) {
            Set<String> filterTypes = new HashSet<>(filters.size());
            for (DescribeComponent filter : filters) {
                filterTypes.add(filter.entityType());
            }
            if (!entityKey.typeSet().equals(filterTypes)) {
                return false;
            }
        }

        for (DescribeComponent filter : filters) {
            if (!matchesDescribeFilter(entityKey, filter)) {
                return false;
            }
        }
        return true;
    }

    private boolean matchesDescribeFilter(EntityKey entityKey, DescribeComponent filter) {
        if (!entityKey.hasType(filter.entityType())) {
            return false;
        }
        String entityName = entityKey.nameOf(filter.entityType());
        return switch (filter.matchType()) {
            case 0 -> Objects.equals(entityName, filter.match()); // EXACT
            case 1 -> entityName == null; // DEFAULT
            case 2 -> entityName != null; // SPECIFIED (must exclude null)
            default -> false;
        };
    }

    private static EntityKey toEntityKey(ClientQuotaEntry entry) {
        boolean hasUser = false;
        String user = null;
        boolean hasClientId = false;
        String clientId = null;
        boolean hasIp = false;
        String ip = null;

        if (entry.getEntity() != null) {
            for (EntityComponent component : entry.getEntity()) {
                if (component == null) {
                    continue;
                }
                switch (component.getType()) {
                    case ENTITY_TYPE_USER:
                        hasUser = true;
                        user = component.getName();
                        break;
                    case ENTITY_TYPE_CLIENT_ID:
                        hasClientId = true;
                        clientId = component.getName();
                        break;
                    case ENTITY_TYPE_IP:
                        hasIp = true;
                        ip = component.getName();
                        break;
                    default:
                        // ignore unknown types, still allow describe to operate on known dimensions
                }
            }
        }

        return new EntityKey(hasUser, user, hasClientId, clientId, hasIp, ip);
    }
}
