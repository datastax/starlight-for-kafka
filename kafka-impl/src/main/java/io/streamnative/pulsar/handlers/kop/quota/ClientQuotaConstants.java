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

import java.util.List;
import java.util.Set;

public final class ClientQuotaConstants {

    public static final String ENTITY_TYPE_USER = "user";
    public static final String ENTITY_TYPE_CLIENT_ID = "client-id";
    public static final String ENTITY_TYPE_IP = "ip";

    public static final List<String> ENTITY_TYPE_CANONICAL_ORDER =
            List.of(ENTITY_TYPE_USER, ENTITY_TYPE_CLIENT_ID, ENTITY_TYPE_IP);

    public static final Set<String> SUPPORTED_ENTITY_TYPES =
            Set.of(ENTITY_TYPE_USER, ENTITY_TYPE_CLIENT_ID, ENTITY_TYPE_IP);

    public static final String QUOTA_PRODUCER_BYTE_RATE = "producer_byte_rate";
    public static final String QUOTA_CONSUMER_BYTE_RATE = "consumer_byte_rate";

    public static final String QUOTA_CONNECTION_CREATION_RATE = "connection_creation_rate";

    private ClientQuotaConstants() {}
}

