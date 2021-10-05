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
package io.streamnative.pulsar.handlers.kop.security.auth;

import com.google.common.base.Joiner;
import lombok.AllArgsConstructor;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;


/**
 * Abstracts Pulsar Metadata Access
 */
public interface PulsarMetadataAccessor {

    @AllArgsConstructor
    class PulsarServiceMetadataAccessor implements PulsarMetadataAccessor {

        private static final String POLICY_ROOT = "/admin/policies/";

        private PulsarService pulsarService;

        private static String path(String... parts) {
            StringBuilder sb = new StringBuilder();
            sb.append(POLICY_ROOT);
            Joiner.on('/').appendTo(sb, parts);
            return sb.toString();
        }

        public CompletableFuture<Optional<TenantInfo>> getTenantInfoAsync(String tenant) {
            return pulsarService.getPulsarResources()
                        .getTenantResources()
                        .getAsync(path(tenant));
        }

        public CompletableFuture<Optional<Policies>> getNamespacePoliciesAsync(NamespaceName namespace) {
                String policiesPath = path(namespace.toString());
                return pulsarService
                        .getPulsarResources()
                        .getNamespaceResources()
                        .getAsync(policiesPath);
        }

        public ServiceConfiguration getConfiguration() {
            return pulsarService.getConfiguration();
        }

    }

    @AllArgsConstructor
    class PulsarAdminMetadataAccessor implements PulsarMetadataAccessor {
        private final Supplier<CompletableFuture<PulsarAdmin>> pulsarAdmin;
        private final ServiceConfiguration serviceConfiguration;

        public CompletableFuture<Optional<TenantInfo>> getTenantInfoAsync(String tenant) {
            return pulsarAdmin.get()
                        .thenCompose(admin -> admin
                                .tenants()
                                .getTenantInfoAsync(tenant)
                                .thenApply(Optional::ofNullable))
                        .exceptionally(error -> {
                            if (error instanceof PulsarAdminException.NotFoundException) {
                                return Optional.empty();
                            } else {
                                throw new CompletionException(error);
                            }});
        }

        public CompletableFuture<Optional<Policies>> getNamespacePoliciesAsync(NamespaceName namespace) {
            return pulsarAdmin.get()
                    .thenCompose(admin -> admin
                            .namespaces()
                            .getPoliciesAsync(namespace.toString())
                            .thenApply(Optional::ofNullable))
                    .exceptionally(error -> {
                        if (error instanceof PulsarAdminException.NotFoundException) {
                            return Optional.empty();
                        } else {
                            throw new CompletionException(error);
                        }});
        }

        public ServiceConfiguration getConfiguration() {
            return serviceConfiguration;
        }
    }

    /**
     * Access TenantInfo for the given Tenant
     * @param tenant
     * @return the TenantInfo or an empty Optional if the tenant does not exist
     */
    CompletableFuture<Optional<TenantInfo>> getTenantInfoAsync(String tenant);

    /**
     * Access Policies for the given Namespace
     * @param namespace
     * @return the Policies or an empty Optional if the namespace does not exist
     */
    CompletableFuture<Optional<Policies>> getNamespacePoliciesAsync(NamespaceName namespace);

    /**
     * Access the service configuration
     * @return the configuration.
     */
    ServiceConfiguration getConfiguration();
}