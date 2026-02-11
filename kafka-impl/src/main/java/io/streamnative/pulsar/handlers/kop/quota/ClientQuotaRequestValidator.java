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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.protocol.Errors;

public final class ClientQuotaRequestValidator {

    @SuppressFBWarnings("EQ_UNUSUAL")
    public record ValidationError(Errors error, String message) {}

    private ClientQuotaRequestValidator() {}

    private static final String MSG_EMPTY_ENTITY = "INVALID_REQUEST: Empty entity is not allowed";

    public static Optional<ValidationError> validateDescribeComponent(String entityType, byte matchType) {
        Optional<ValidationError> typeError = validateSupportedEntityType(entityType);
        if (typeError.isPresent()) {
            return typeError;
        }
        if (matchType != 0 && matchType != 1 && matchType != 2) {
            return Optional.of(new ValidationError(
                    Errors.INVALID_REQUEST,
                    "INVALID_REQUEST: Unsupported match_type " + matchType + ". Supported: 0, 1, 2"));
        }
        return Optional.empty();
    }

    public static Optional<ValidationError> validateAlterEntity(List<EntityComponent> entity,
                                                                boolean allowAlterEmptyClientId) {
        if (entity == null || entity.isEmpty()) {
            return Optional.of(new ValidationError(Errors.INVALID_REQUEST, MSG_EMPTY_ENTITY));
        }
        Set<String> types = new HashSet<>(entity.size());
        boolean hasComponent = false;
        for (EntityComponent component : entity) {
            if (component == null) {
                continue;
            }
            hasComponent = true;
            String entityType = component.getType();
            String entityName = component.getName();

            Optional<ValidationError> typeError = validateSupportedEntityType(entityType);
            if (typeError.isPresent()) {
                return typeError;
            }
            if (!types.add(entityType)) {
                return Optional.of(new ValidationError(
                        Errors.INVALID_REQUEST,
                        "INVALID_REQUEST: Duplicate entityType in entity: " + entityType));
            }
            if ("".equals(entityName)) {
                if (ClientQuotaConstants.ENTITY_TYPE_USER.equals(entityType)
                        || ClientQuotaConstants.ENTITY_TYPE_IP.equals(entityType)) {
                    return Optional.of(new ValidationError(
                            Errors.INVALID_REQUEST,
                            "INVALID_REQUEST: Empty entity_name is not allowed for entityType " + entityType));
                }
                if (ClientQuotaConstants.ENTITY_TYPE_CLIENT_ID.equals(entityType) && !allowAlterEmptyClientId) {
                    return Optional.of(new ValidationError(
                            Errors.INVALID_REQUEST,
                            "INVALID_REQUEST: Empty entity_name is not allowed for entityType client-id"));
                }
            }
        }
        if (!hasComponent) {
            return Optional.of(new ValidationError(Errors.INVALID_REQUEST, MSG_EMPTY_ENTITY));
        }
        return Optional.empty();
    }

    public static Optional<ValidationError> validateQuotaValue(String quotaKey,
                                                               double value,
                                                               boolean remove) {
        if (remove) {
            return Optional.empty();
        }
        if (!Double.isFinite(value) || value < 0.0d) {
            return Optional.of(new ValidationError(
                    Errors.INVALID_REQUEST,
                    "INVALID_REQUEST: Invalid quota value for key " + quotaKey + ": " + value
                            + ". Must be finite and >= 0"));
        }
        return Optional.empty();
    }

    public static Optional<ValidationError> validateNoDuplicateQuotaKeys(List<String> quotaKeys) {
        if (quotaKeys == null) {
            return Optional.empty();
        }
        Set<String> seen = new HashSet<>(quotaKeys.size());
        for (String key : quotaKeys) {
            if (!seen.add(key)) {
                return Optional.of(new ValidationError(
                        Errors.INVALID_REQUEST,
                        "INVALID_REQUEST: Duplicate quota key in entity"));
            }
        }
        return Optional.empty();
    }

    public static Optional<ValidationError> validateDuplicateEntityInRequest(String entityKey, Set<String> seenKeys) {
        Objects.requireNonNull(seenKeys, "seenKeys");
        if (!seenKeys.add(entityKey)) {
            return Optional.of(new ValidationError(Errors.INVALID_REQUEST, "Duplicate entity in request"));
        }
        return Optional.empty();
    }

    public static Optional<ValidationError> validateSupportedEntityType(String entityType) {
        if (!ClientQuotaConstants.SUPPORTED_ENTITY_TYPES.contains(entityType)) {
            return Optional.of(new ValidationError(
                    Errors.INVALID_REQUEST,
                    "INVALID_REQUEST: Unsupported entityType " + entityType + ". Supported: user, client-id, ip"));
        }
        return Optional.empty();
    }
}
