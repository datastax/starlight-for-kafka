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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;

/**
 * Abstracts Pulsar Metadata Access.
 */
public interface PulsarMetadataAccessor {

    @AllArgsConstructor
    class PulsarServiceMetadataAccessor implements PulsarMetadataAccessor {

        private static final String POLICY_ROOT = "/admin/policies/";

        private PulsarService pulsarService;

        public CompletableFuture<Optional<TenantInfo>> getTenantInfoAsync(String tenant) {
            return pulsarService.getPulsarResources()
                        .getTenantResources()
                        .getTenantAsync(tenant);
        }

        public CompletableFuture<Optional<Policies>> getNamespacePoliciesAsync(NamespaceName namespace) {
                return pulsarService
                        .getPulsarResources()
                        .getNamespaceResources()
                        .getPoliciesAsync(namespace.getNamespaceObject());
        }

        public ServiceConfiguration getConfiguration() {
            return pulsarService.getConfiguration();
        }

        @Override
        public AuthorizationService getAuthorizationService() {
            return pulsarService.getBrokerService().getAuthorizationService();
        }
    }

    @AllArgsConstructor
    class PulsarAdminMetadataAccessor implements PulsarMetadataAccessor {
        private final Supplier<CompletableFuture<PulsarAdmin>> pulsarAdmin;
        private final ServiceConfiguration serviceConfiguration;
        private final AuthorizationService authorizationService;

        @Override
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
                            }
                        });
        }

        @Override
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
                        }
                    });
        }

        @Override
        public ServiceConfiguration getConfiguration() {
            return serviceConfiguration;
        }
        @Override
        public AuthorizationService getAuthorizationService() {
            return authorizationService;
        }
    }

    /**
     * Access TenantInfo for the given Tenant.
     * @param tenant
     * @return the TenantInfo or an empty Optional if the tenant does not exist
     */
    CompletableFuture<Optional<TenantInfo>> getTenantInfoAsync(String tenant);

    /**
     * Access Policies for the given Namespace.
     * @param namespace
     * @return the Policies or an empty Optional if the namespace does not exist
     */
    CompletableFuture<Optional<Policies>> getNamespacePoliciesAsync(NamespaceName namespace);

    /**
     * Access the service configuration.
     * @return the configuration.
     */
    ServiceConfiguration getConfiguration();

    /**
     * Access the AuthorizationService.
     * @return the AuthorizationService.
     */
    AuthorizationService getAuthorizationService();
}