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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.Test;

public class ClientQuotaRequestValidatorTest {

    @Test
    public void testAlterEmptyEntityRejected() {
        Optional<ClientQuotaRequestValidator.ValidationError> error =
                ClientQuotaRequestValidator.validateAlterEntity(List.of(), false);
        assertTrue(error.isPresent());
        assertEquals(error.get().message(), "INVALID_REQUEST: Empty entity is not allowed");
    }

    @Test
    public void testUnsupportedEntityTypeMessage() {
        Optional<ClientQuotaRequestValidator.ValidationError> error =
                ClientQuotaRequestValidator.validateSupportedEntityType("unknown");
        assertTrue(error.isPresent());
        assertEquals(error.get().message(),
                "INVALID_REQUEST: Unsupported entityType unknown. Supported: user, client-id, ip");
    }

    @Test
    public void testUnsupportedMatchTypeMessage() {
        Optional<ClientQuotaRequestValidator.ValidationError> error =
                ClientQuotaRequestValidator.validateDescribeComponent("user", (byte) 3);
        assertTrue(error.isPresent());
        assertEquals(error.get().message(), "INVALID_REQUEST: Unsupported match_type 3. Supported: 0, 1, 2");
    }

    @Test
    public void testAlterEmptyEntityNameRules() {
        Optional<ClientQuotaRequestValidator.ValidationError> userEmpty =
                ClientQuotaRequestValidator.validateAlterEntity(List.of(new EntityComponent("user", "")), false);
        assertTrue(userEmpty.isPresent());
        assertEquals(userEmpty.get().message(),
                "INVALID_REQUEST: Empty entity_name is not allowed for entityType user");

        Optional<ClientQuotaRequestValidator.ValidationError> ipEmpty =
                ClientQuotaRequestValidator.validateAlterEntity(List.of(new EntityComponent("ip", "")), false);
        assertTrue(ipEmpty.isPresent());
        assertEquals(ipEmpty.get().message(), "INVALID_REQUEST: Empty entity_name is not allowed for entityType ip");

        Optional<ClientQuotaRequestValidator.ValidationError> clientIdEmpty =
                ClientQuotaRequestValidator.validateAlterEntity(List.of(new EntityComponent("client-id", "")), false);
        assertTrue(clientIdEmpty.isPresent());
        assertEquals(clientIdEmpty.get().message(),
                "INVALID_REQUEST: Empty entity_name is not allowed for entityType client-id");

        Optional<ClientQuotaRequestValidator.ValidationError> clientIdEmptyAllowed =
                ClientQuotaRequestValidator.validateAlterEntity(List.of(new EntityComponent("client-id", "")), true);
        assertFalse(clientIdEmptyAllowed.isPresent());
    }

    @Test
    public void testDuplicateEntityType() {
        Optional<ClientQuotaRequestValidator.ValidationError> error =
                ClientQuotaRequestValidator.validateAlterEntity(List.of(
                        new EntityComponent("user", "alice"),
                        new EntityComponent("user", "bob")
                ), false);
        assertTrue(error.isPresent());
        assertEquals(error.get().message(), "INVALID_REQUEST: Duplicate entityType in entity: user");
    }

    @Test
    public void testDuplicateQuotaKey() {
        Optional<ClientQuotaRequestValidator.ValidationError> error =
                ClientQuotaRequestValidator.validateNoDuplicateQuotaKeys(List.of("k1", "k1"));
        assertTrue(error.isPresent());
        assertEquals(error.get().message(), "INVALID_REQUEST: Duplicate quota key in entity");
    }

    @Test
    public void testDuplicateEntityInRequest() {
        Set<String> seen = new java.util.HashSet<>();
        assertFalse(ClientQuotaRequestValidator
                .validateDuplicateEntityInRequest("user=alice", seen)
                .isPresent());
        Optional<ClientQuotaRequestValidator.ValidationError> error =
                ClientQuotaRequestValidator.validateDuplicateEntityInRequest("user=alice", seen);
        assertTrue(error.isPresent());
        assertEquals(error.get().message(), "Duplicate entity in request");
    }

    @Test
    public void testQuotaValueValidation() {
        Optional<ClientQuotaRequestValidator.ValidationError> nan =
                ClientQuotaRequestValidator.validateQuotaValue("k1", Double.NaN, false);
        assertTrue(nan.isPresent());
        assertEquals(nan.get().message(),
                "INVALID_REQUEST: Invalid quota value for key k1: NaN. Must be finite and >= 0");

        Optional<ClientQuotaRequestValidator.ValidationError> inf =
                ClientQuotaRequestValidator.validateQuotaValue("k1", Double.POSITIVE_INFINITY, false);
        assertTrue(inf.isPresent());
        assertEquals(inf.get().message(),
                "INVALID_REQUEST: Invalid quota value for key k1: Infinity. Must be finite and >= 0");

        Optional<ClientQuotaRequestValidator.ValidationError> negative =
                ClientQuotaRequestValidator.validateQuotaValue("k1", -1.0d, false);
        assertTrue(negative.isPresent());
        assertEquals(negative.get().message(),
                "INVALID_REQUEST: Invalid quota value for key k1: -1.0. Must be finite and >= 0");

        assertFalse(ClientQuotaRequestValidator.validateQuotaValue("k1", 0.0d, false).isPresent());
        assertFalse(ClientQuotaRequestValidator.validateQuotaValue("k1", 1.0d, false).isPresent());

        // remove=true should skip value validation
        assertFalse(ClientQuotaRequestValidator.validateQuotaValue("k1", Double.NaN, true).isPresent());
    }
}
