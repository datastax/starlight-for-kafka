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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public final class ClientQuotaEntityUtils {

    private ClientQuotaEntityUtils() {}

    public record EntityComponentKey(String type, String name) {}

    public record CanonicalEntityKey(List<EntityComponentKey> components) {
        public CanonicalEntityKey {
            components = components == null ? List.of() : List.copyOf(components);
        }
    }

    public static List<EntityComponent> canonicalize(List<EntityComponent> entity) {
        if (entity == null || entity.isEmpty()) {
            return Collections.emptyList();
        }
        List<EntityComponent> copy = new ArrayList<>(entity.size());
        for (EntityComponent component : entity) {
            if (component != null) {
                copy.add(component);
            }
        }
        copy.sort(Comparator
                .comparingInt((EntityComponent c) -> typeOrder(c.getType()))
                .thenComparing(EntityComponent::getType, Comparator.nullsFirst(String::compareTo)));
        return copy;
    }

    public static CanonicalEntityKey canonicalKey(List<EntityComponent> entity) {
        return canonicalKeyOfCanonicalEntity(canonicalize(entity));
    }

    public static CanonicalEntityKey canonicalKeyOfCanonicalEntity(List<EntityComponent> canonicalEntity) {
        if (canonicalEntity == null || canonicalEntity.isEmpty()) {
            return new CanonicalEntityKey(List.of());
        }
        List<EntityComponentKey> keys = new ArrayList<>(canonicalEntity.size());
        for (EntityComponent component : canonicalEntity) {
            if (component == null) {
                continue;
            }
            keys.add(new EntityComponentKey(component.getType(), component.getName()));
        }
        return new CanonicalEntityKey(keys);
    }

    public static int compareCanonicalKeys(CanonicalEntityKey left, CanonicalEntityKey right) {
        if (left == right) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        List<EntityComponentKey> leftComponents = left.components();
        List<EntityComponentKey> rightComponents = right.components();
        int min = Math.min(leftComponents.size(), rightComponents.size());
        for (int i = 0; i < min; i++) {
            int cmp = compareComponentKeys(leftComponents.get(i), rightComponents.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(leftComponents.size(), rightComponents.size());
    }

    private static int compareComponentKeys(EntityComponentKey left, EntityComponentKey right) {
        if (left == right) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        int typeOrderCompare = Integer.compare(typeOrder(left.type()), typeOrder(right.type()));
        if (typeOrderCompare != 0) {
            return typeOrderCompare;
        }
        int typeCompare = compareNullableString(left.type(), right.type());
        if (typeCompare != 0) {
            return typeCompare;
        }
        return compareNullableString(left.name(), right.name());
    }

    private static int compareNullableString(String left, String right) {
        if (left == right) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        return left.compareTo(right);
    }

    public static String canonicalEntityString(List<EntityComponent> entity) {
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

    public static int typeOrder(String entityType) {
        if (entityType == null) {
            return Integer.MAX_VALUE;
        }
        int idx = ENTITY_TYPE_CANONICAL_ORDER.indexOf(entityType);
        return idx >= 0 ? idx : Integer.MAX_VALUE;
    }
}
