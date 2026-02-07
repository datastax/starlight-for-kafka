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
/**
 * Implementation of Kafka client quotas for KoP (Kafka on Pulsar).
 *
 * <p>This package provides the infrastructure for enforcing client quotas in KoP,
 * including producer/consumer byte rate limits and connection rate limits.
 * It implements the quota resolution hierarchy, sliding window rate limiters,
 * and quota management APIs compatible with Kafka's client quota specification.
 */
package io.streamnative.pulsar.handlers.kop.quota;
