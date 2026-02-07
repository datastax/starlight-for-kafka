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

import io.streamnative.pulsar.handlers.kop.quota.ClientQuotaIndex.DescribeComponent;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class ClientQuotaIndexTest {

    @Test
    public void testDescribeMatchTypesAndStrict() {
        ClientQuotaSnapshot snapshot = new ClientQuotaSnapshot(1, List.of(
                new ClientQuotaEntry(List.of(new EntityComponent("user", "alice")), Map.of("k1", 1.0d)),
                new ClientQuotaEntry(List.of(new EntityComponent("user", "alice"), new EntityComponent("client-id",
                        null)), Map.of("k2", 2.0d)),
                new ClientQuotaEntry(List.of(new EntityComponent("user", "alice"), new EntityComponent("client-id",
                        "c1")), Map.of("k3", 3.0d)),
                new ClientQuotaEntry(List.of(new EntityComponent("client-id", "c1")), Map.of("k4", 4.0d)),
                new ClientQuotaEntry(List.of(new EntityComponent("user", "alice"), new EntityComponent("ip",
                        "1.1.1.1")), Map.of("k5", 5.0d))
        ));

        ClientQuotaIndex index = ClientQuotaIndex.ofSnapshot(snapshot);

        // EXACT user=alice (non-strict) matches entities that include user=alice even if they have extra types.
        List<ClientQuotaEntry> exactUser = index.describe(
                List.of(new DescribeComponent("user", (byte) 0, "alice")),
                false);
        assertEquals(entityKeySet(exactUser), Set.of(
                "user=alice",
                "user=alice|client-id=<default>",
                "user=alice|client-id=c1",
                "user=alice|ip=1.1.1.1"
        ));

        // DEFAULT client-id matches entities that include client-id=null only.
        List<ClientQuotaEntry> defaultClientId = index.describe(
                List.of(new DescribeComponent("client-id", (byte) 1, null)),
                false);
        assertEquals(entityKeySet(defaultClientId), Set.of("user=alice|client-id=<default>"));

        // SPECIFIED client-id must exclude null (but includes empty string).
        List<ClientQuotaEntry> specifiedClientId = index.describe(
                List.of(new DescribeComponent("client-id", (byte) 2, null)),
                false);
        assertEquals(entityKeySet(specifiedClientId), Set.of(
                "user=alice|client-id=c1",
                "client-id=c1"
        ));

        // strict=true compares type sets (ignoring order)
        List<ClientQuotaEntry> strictUserOnly = index.describe(
                List.of(new DescribeComponent("user", (byte) 0, "alice")),
                true);
        assertEquals(entityKeySet(strictUserOnly), Set.of("user=alice"));

        List<ClientQuotaEntry> strictUserClient = index.describe(
                List.of(
                        new DescribeComponent("user", (byte) 0, "alice"),
                        new DescribeComponent("client-id", (byte) 2, null)
                ),
                true);
        assertEquals(entityKeySet(strictUserClient), Set.of("user=alice|client-id=c1"));
    }

    private static Set<String> entityKeySet(List<ClientQuotaEntry> entries) {
        return entries.stream()
                .map(e -> ClientQuotaEntityUtils.canonicalEntityString(ClientQuotaEntityUtils.canonicalize(e.getEntity())))
                .collect(Collectors.toSet());
    }
}

