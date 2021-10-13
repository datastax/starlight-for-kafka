package io.streamnative.pulsar.handlers.kop;

import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryChannelInitializer;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryHandler;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryRequestAuthenticator;
import io.streamnative.pulsar.handlers.kop.security.auth.Authorizer;
import io.streamnative.pulsar.handlers.kop.security.auth.PulsarMetadataAccessor;
import io.streamnative.pulsar.handlers.kop.security.auth.SimpleAclAuthorizer;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.admin.PulsarAdmin;

public class KafkaSchemaRegistryProxyManager {

    private final KafkaServiceConfiguration kafkaConfig;
    private final AuthenticationService authenticationService;
    private final Supplier<CompletableFuture<PulsarAdmin>> pulsarAdmin;
    private final Supplier<String> brokerUrlSupplier;
    private final ProxySchemaRegistryHttpRequestProcessor proxy;

    public KafkaSchemaRegistryProxyManager(KafkaServiceConfiguration kafkaConfig,
                                           Supplier<String> brokerUrlSupplier,
                                           Supplier<CompletableFuture<PulsarAdmin>>  systemPulsarAdmin,
                                           AuthenticationService authenticationService) {
        this.kafkaConfig = kafkaConfig;
        this.pulsarAdmin = systemPulsarAdmin;
        this.brokerUrlSupplier = brokerUrlSupplier;
        this.authenticationService = authenticationService;
        if (kafkaConfig.isKopSchemaRegistryEnable()) {
            Authorizer authorizer = new SimpleAclAuthorizer(new PulsarMetadataAccessor.PulsarAdminMetadataAccessor(
                    systemPulsarAdmin, kafkaConfig));
            SchemaRegistryRequestAuthenticator schemaRegistryRequestAuthenticator
                    = new SchemaRegistryManager.HttpRequestAuthenticator(kafkaConfig,
                    authenticationService, authorizer);
            this.proxy = new ProxySchemaRegistryHttpRequestProcessor(brokerUrlSupplier,
                    kafkaConfig,
                    schemaRegistryRequestAuthenticator);
        } else {
            this.proxy = null;
        }
    }

    public InetSocketAddress getAddress() {
        return new InetSocketAddress(kafkaConfig.getKopSchemaRegistryProxyPort());
    }

    public Optional<SchemaRegistryChannelInitializer> build() throws Exception {
        if (!kafkaConfig.isKopSchemaRegistryEnable()) {
            return Optional.empty();
        }
        SchemaRegistryHandler handler = new SchemaRegistryHandler();

        handler.addProcessor(proxy);
        return Optional.of(new SchemaRegistryChannelInitializer(handler));
    }

    public void close() {
        // release HTTP Client
        if (proxy != null) {
            proxy.close();
        }
    }
}
