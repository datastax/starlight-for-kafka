package io.streamnative.pulsar.handlers.kop;

import static io.streamnative.pulsar.handlers.kop.KafkaProtocolHandler.TLS_HANDLER;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryChannelInitializer;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryHandler;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryRequestAuthenticator;
import io.streamnative.pulsar.handlers.kop.security.auth.Authorizer;
import io.streamnative.pulsar.handlers.kop.security.auth.PulsarMetadataAccessor;
import io.streamnative.pulsar.handlers.kop.security.auth.SimpleAclAuthorizer;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.util.NettyServerSslContextBuilder;
import org.apache.pulsar.common.util.keystoretls.NettySSLContextAutoRefreshBuilder;

public class KafkaSchemaRegistryProxyManager {

    private final KafkaServiceConfiguration kafkaConfig;
    private final AuthenticationService authenticationService;
    private final Supplier<CompletableFuture<PulsarAdmin>> pulsarAdmin;
    private final Supplier<String> brokerUrlSupplier;
    private final ProxySchemaRegistryHttpRequestProcessor proxy;
    private final NettySSLContextAutoRefreshBuilder serverSSLContextAutoRefreshBuilder;
    private final NettyServerSslContextBuilder serverSslCtxRefresher;

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
            // we are using Pulsar Proxy TLS configuration, not KOP
            // this way we can use the same configuration of conf/proxy.conf
            boolean enableTls = kafkaConfig.isKopSchemaRegistryProxyEnableTls();
            if (enableTls) {
                if (kafkaConfig.isTlsEnabledWithKeyStore()) {
                    serverSSLContextAutoRefreshBuilder = new NettySSLContextAutoRefreshBuilder(
                            kafkaConfig.getTlsProvider(),
                            kafkaConfig.getTlsKeyStoreType(),
                            kafkaConfig.getTlsKeyStore(),
                            kafkaConfig.getTlsKeyStorePassword(),
                            kafkaConfig.isTlsAllowInsecureConnection(),
                            kafkaConfig.getTlsTrustStoreType(),
                            kafkaConfig.getTlsTrustStore(),
                            kafkaConfig.getTlsTrustStorePassword(),
                            kafkaConfig.isTlsRequireTrustedClientCertOnConnect(),
                            kafkaConfig.getTlsCiphers(),
                            kafkaConfig.getTlsProtocols(),
                            kafkaConfig.getTlsCertRefreshCheckDurationSec());
                    serverSslCtxRefresher = null;
                } else {
                    serverSSLContextAutoRefreshBuilder = null;
                    serverSslCtxRefresher = new NettyServerSslContextBuilder(kafkaConfig.isTlsAllowInsecureConnection(),
                            kafkaConfig.getTlsTrustCertsFilePath(), kafkaConfig.getTlsCertificateFilePath(),
                            kafkaConfig.getTlsKeyFilePath(), kafkaConfig.getTlsCiphers(), kafkaConfig.getTlsProtocols(),
                            kafkaConfig.isTlsRequireTrustedClientCertOnConnect(),
                            kafkaConfig.getTlsCertRefreshCheckDurationSec());
                }
            } else {
                this.serverSslCtxRefresher = null;
                this.serverSSLContextAutoRefreshBuilder = null;
            }
        } else {
            this.serverSslCtxRefresher = null;
            this.serverSSLContextAutoRefreshBuilder = null;
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
        if (kafkaConfig.isKopSchemaRegistryProxyEnableTls()) {
            tlsConfigurator = (pipeline -> {
                if (serverSslCtxRefresher != null) {
                    SslContext sslContext = serverSslCtxRefresher.get();
                    if (sslContext != null) {
                        pipeline.addLast(TLS_HANDLER, sslContext.newHandler(pipeline.channel().alloc()));
                    }
                } else if (kafkaConfig.isTlsEnabledWithKeyStore() && serverSSLContextAutoRefreshBuilder != null) {
                    pipeline.addLast(TLS_HANDLER,
                            new SslHandler(serverSSLContextAutoRefreshBuilder.get().createSSLEngine()));
                }
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
