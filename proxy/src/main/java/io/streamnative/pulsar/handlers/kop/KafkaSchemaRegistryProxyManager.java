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
package io.streamnative.pulsar.handlers.kop;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryChannelInitializer;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryHandler;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryRequestAuthenticator;
import io.streamnative.pulsar.handlers.kop.security.auth.Authorizer;
import io.streamnative.pulsar.handlers.kop.security.auth.PulsarMetadataAccessor;
import io.streamnative.pulsar.handlers.kop.security.auth.SimpleAclAuthorizer;
import io.streamnative.pulsar.handlers.kop.utils.ssl.SSLUtils;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.client.admin.PulsarAdmin;

public class KafkaSchemaRegistryProxyManager {

    private final KafkaServiceConfiguration kafkaConfig;
    private final ProxySchemaRegistryHttpRequestProcessor proxy;
    private final SSLUtils.ServerSideTLSSupport tlsSupport;

    public KafkaSchemaRegistryProxyManager(KafkaServiceConfiguration kafkaConfig,
                                           Supplier<String> brokerUrlSupplier,
                                           Supplier<CompletableFuture<PulsarAdmin>> systemPulsarAdmin,
                                           AuthenticationService authenticationService,
                                           AuthorizationService authorizationService) {
        this.kafkaConfig = kafkaConfig;
        if (kafkaConfig.isKopSchemaRegistryEnable()) {
            Authorizer authorizer = new SimpleAclAuthorizer(new PulsarMetadataAccessor.PulsarAdminMetadataAccessor(
                    systemPulsarAdmin, kafkaConfig, authorizationService));
            SchemaRegistryRequestAuthenticator schemaRegistryRequestAuthenticator =
                    new SchemaRegistryManager.HttpRequestAuthenticator(kafkaConfig,
                    authenticationService, authorizer);
            this.proxy = new ProxySchemaRegistryHttpRequestProcessor(brokerUrlSupplier,
                    kafkaConfig,
                    schemaRegistryRequestAuthenticator);
            // we are using Pulsar Proxy TLS configuration, not KOP
            // this way we can use the same configuration of conf/proxy.conf
            boolean enableTls = kafkaConfig.isKopSchemaRegistryProxyEnableTls();
            if (enableTls) {
                tlsSupport = new SSLUtils.ServerSideTLSSupport(kafkaConfig);
            } else {
                tlsSupport = null;
            }
        } else {
            this.tlsSupport = null;
            this.proxy = null;
        }
    }

    public InetSocketAddress getAddress() {
        return new InetSocketAddress(kafkaConfig.getKopSchemaRegistryProxyPort());
    }

    public Optional<ChannelInitializer<SocketChannel>> build() throws Exception {
        if (!kafkaConfig.isKopSchemaRegistryEnable()) {
            return Optional.empty();
        }
        Consumer<ChannelPipeline> tlsConfigurator;
        if (tlsSupport != null) {
            tlsConfigurator = (pipeline -> {
                tlsSupport.addTlsHandler((SocketChannel) pipeline.channel());
            });
        } else {
            tlsConfigurator = null;
        }
        SchemaRegistryHandler handler = new SchemaRegistryHandler();
        handler.addProcessor(proxy);
        return Optional.of(new SchemaRegistryChannelInitializer(handler, tlsConfigurator));
    }

    public void close() {
        // release HTTP Client
        if (proxy != null) {
            proxy.close();
        }
    }
}
