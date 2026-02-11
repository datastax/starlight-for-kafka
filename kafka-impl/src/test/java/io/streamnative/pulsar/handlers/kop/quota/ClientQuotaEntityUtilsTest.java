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
import static org.testng.Assert.assertNotEquals;

import java.util.List;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.testng.annotations.Test;

public class ClientQuotaEntityUtilsTest {

    @Test
    public void testCanonicalKeyAvoidsSeparatorCollisions() {
        List<EntityComponent> entityA = List.of(
                new EntityComponent(ClientQuotaConstants.ENTITY_TYPE_USER, "alice|client-id=bob"));
        List<EntityComponent> entityB = List.of(
                new EntityComponent(ClientQuotaConstants.ENTITY_TYPE_USER, "alice"),
                new EntityComponent(ClientQuotaConstants.ENTITY_TYPE_CLIENT_ID, "bob"));

        ClientQuotaEntity keyA = ClientQuotaEntityUtils.toClientQuotaEntity(entityA);
        ClientQuotaEntity keyB = ClientQuotaEntityUtils.toClientQuotaEntity(entityB);
        assertNotEquals(keyA, keyB);
    }

    @Test
    public void testCanonicalKeyDistinguishesNullAndLiteralDefault() {
        List<EntityComponent> entityDefault = List.of(
                new EntityComponent(ClientQuotaConstants.ENTITY_TYPE_CLIENT_ID, null));
        List<EntityComponent> entityLiteral = List.of(
                new EntityComponent(ClientQuotaConstants.ENTITY_TYPE_CLIENT_ID, "<default>"));

        ClientQuotaEntity keyDefault = ClientQuotaEntityUtils.toClientQuotaEntity(entityDefault);
        ClientQuotaEntity keyLiteral = ClientQuotaEntityUtils.toClientQuotaEntity(entityLiteral);
        assertNotEquals(keyDefault, keyLiteral);
    }

    @Test
    public void testCanonicalKeyIsOrderIndependent() {
        List<EntityComponent> entity1 = List.of(
                new EntityComponent(ClientQuotaConstants.ENTITY_TYPE_CLIENT_ID, "c1"),
                new EntityComponent(ClientQuotaConstants.ENTITY_TYPE_USER, "alice"));
        List<EntityComponent> entity2 = List.of(
                new EntityComponent(ClientQuotaConstants.ENTITY_TYPE_USER, "alice"),
                new EntityComponent(ClientQuotaConstants.ENTITY_TYPE_CLIENT_ID, "c1"));

        assertEquals(ClientQuotaEntityUtils.toClientQuotaEntity(entity1),
                ClientQuotaEntityUtils.toClientQuotaEntity(entity2));
    }
}
