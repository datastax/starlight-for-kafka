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

import static io.streamnative.pulsar.handlers.kop.KafkaProtocolHandler.TLS_HANDLER;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.Node;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.common.util.NettyServerSslContextBuilder;
import org.apache.pulsar.common.util.keystoretls.NettySSLContextAutoRefreshBuilder;

/**
 * A channel initializer that initialize channels for kafka protocol.
 */
public class KafkaProxyChannelInitializer extends ChannelInitializer<SocketChannel> {

    private static final Logger log = LoggerFactory.getLogger(KafkaProxyChannelInitializer.class);

    public static final int MAX_FRAME_LENGTH = 100 * 1024 * 1024; // 100MB

    @Getter
    private final KafkaProtocolProxyMain.PulsarAdminProvider pulsarAdmin;
    @Getter
    private final AuthenticationService authenticationService;
    @Getter
    private final KafkaServiceConfiguration kafkaConfig;

    @Getter
    private final boolean enableTls;
    @Getter
    private final EndPoint advertisedEndPoint;
    private final NettyServerSslContextBuilder serverSslCtxRefresher;
    private final boolean tlsEnabledWithKeyStore;
    private final RequestStats requestStats;
    private final Function<String, String> brokerAddressMapper;
    private final ConcurrentHashMap<String, Node> topicsLeaders;
    private NettySSLContextAutoRefreshBuilder serverSSLContextAutoRefreshBuilder;

    public KafkaProxyChannelInitializer(
            KafkaProtocolProxyMain.PulsarAdminProvider pulsarAdmin,
            AuthenticationService authenticationService,
            KafkaServiceConfiguration serviceConfig,
            boolean enableTLS,
            EndPoint advertisedEndPoint,
            Function<String, String> brokerAddressMapper,
            ConcurrentHashMap<String, Node> topicsLeaders,
            RequestStats requestStats) {
        super();
        this.topicsLeaders = topicsLeaders;
        this.requestStats = requestStats;
        this.brokerAddressMapper = brokerAddressMapper;
        this.authenticationService = authenticationService;
        this.pulsarAdmin = pulsarAdmin;
        this.kafkaConfig = serviceConfig;
        this.enableTls = enableTLS;
        this.advertisedEndPoint = advertisedEndPoint;
        this.tlsEnabledWithKeyStore = serviceConfig.isTlsEnabledWithKeyStore();

        // we are using Pulsar Proxy TLS configuration, not KOP
        // this way we can use the same configuration of conf/proxy.conf
        if (enableTls) {
            if (tlsEnabledWithKeyStore) {
                serverSSLContextAutoRefreshBuilder = new NettySSLContextAutoRefreshBuilder(
                        serviceConfig.getTlsProvider(),
                        serviceConfig.getTlsKeyStoreType(),
                        serviceConfig.getTlsKeyStore(),
                        serviceConfig.getTlsKeyStorePassword(),
                        serviceConfig.isTlsAllowInsecureConnection(),
                        serviceConfig.getTlsTrustStoreType(),
                        serviceConfig.getTlsTrustStore(),
                        serviceConfig.getTlsTrustStorePassword(),
                        serviceConfig.isTlsRequireTrustedClientCertOnConnect(),
                        serviceConfig.getTlsCiphers(),
                        serviceConfig.getTlsProtocols(),
                        serviceConfig.getTlsCertRefreshCheckDurationSec());
                serverSslCtxRefresher = null;
            } else {
                serverSSLContextAutoRefreshBuilder = null;
                serverSslCtxRefresher = buildNettyServerSslContextBuilder(serviceConfig);
            }
        } else {
            this.serverSslCtxRefresher = null;
        }
    }

    public static NettyServerSslContextBuilder buildNettyServerSslContextBuilder(KafkaServiceConfiguration serviceConfig) {
        try {
            try {
                Constructor<NettyServerSslContextBuilder> constructor283 =
                        NettyServerSslContextBuilder.class.getConstructor(
                                SslProvider.class,
                                Boolean.TYPE,
                                String.class,
                                String.class,
                                String.class,
                                Set.class,
                                Set.class,
                                Boolean.TYPE,
                                Long.TYPE);
                SslProvider sslProvider = null;
                if (serviceConfig.getTlsProvider() != null) {
                    sslProvider = SslProvider.valueOf(serviceConfig.getTlsProvider());
                }
                return constructor283.newInstance(
                        sslProvider,
                        serviceConfig.isTlsAllowInsecureConnection(),
                        serviceConfig.getTlsTrustCertsFilePath(), serviceConfig.getTlsCertificateFilePath(),
                        serviceConfig.getTlsKeyFilePath(), serviceConfig.getTlsCiphers(),
                        serviceConfig.getTlsProtocols(),
                        serviceConfig.isTlsRequireTrustedClientCertOnConnect(),
                        serviceConfig.getTlsCertRefreshCheckDurationSec());
            } catch (NoSuchMethodException fallbackTo2880) {
                Constructor<NettyServerSslContextBuilder> constructor280 =
                        NettyServerSslContextBuilder.class.getConstructor(
                                Boolean.TYPE,
                                String.class,
                                String.class,
                                String.class,
                                Set.class,
                                Set.class,
                                Boolean.TYPE,
                                Long.TYPE);
                return constructor280.newInstance(serviceConfig.isTlsAllowInsecureConnection(),
                        serviceConfig.getTlsTrustCertsFilePath(), serviceConfig.getTlsCertificateFilePath(),
                        serviceConfig.getTlsKeyFilePath(), serviceConfig.getTlsCiphers(),
                        serviceConfig.getTlsProtocols(),
                        serviceConfig.isTlsRequireTrustedClientCertOnConnect(),
                        serviceConfig.getTlsCertRefreshCheckDurationSec());
            }
        } catch (Throwable t) {
            Arrays.asList(NettyServerSslContextBuilder.class.getConstructors()).forEach(c -> {
                log.info("Available constructor: {}", c);
            });
            throw new RuntimeException(t);
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        if (this.enableTls) {
            if (serverSslCtxRefresher != null) {
                SslContext sslContext = serverSslCtxRefresher.get();
                if (sslContext != null) {
                    ch.pipeline().addLast(TLS_HANDLER, sslContext.newHandler(ch.alloc()));
                }
            } else if (this.tlsEnabledWithKeyStore && serverSSLContextAutoRefreshBuilder != null) {
                ch.pipeline().addLast(TLS_HANDLER,
                        new SslHandler(serverSSLContextAutoRefreshBuilder.get().createSSLEngine()));
            }
        }
        String id = ch.remoteAddress() + "";
        ch.pipeline().addLast(new LengthFieldPrepender(4));
        ch.pipeline().addLast("frameDecoder",
                new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
        ch.pipeline().addLast("handler",
                new KafkaProxyRequestHandler(id, pulsarAdmin, authenticationService, kafkaConfig,
                        // use the same eventloop to preserve ordering
                        enableTls, advertisedEndPoint, brokerAddressMapper, ch.eventLoop(),
                        requestStats, topicsLeaders));
    }

}
