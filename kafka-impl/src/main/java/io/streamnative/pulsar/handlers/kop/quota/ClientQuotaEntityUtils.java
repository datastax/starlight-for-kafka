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
        copy.sort(Comparator.comparingInt(c -> typeOrder(c.getType())));
        return copy;
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

