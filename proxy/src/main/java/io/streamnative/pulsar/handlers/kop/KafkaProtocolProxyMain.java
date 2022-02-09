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

import static com.google.common.base.Preconditions.checkState;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.SERVER_SCOPE;

import com.google.common.collect.ImmutableMap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.pulsar.handlers.kop.stats.PrometheusMetricsProvider;
import io.streamnative.pulsar.handlers.kop.stats.StatsLogger;
import io.streamnative.pulsar.handlers.kop.utils.ConfigurationUtils;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.AuthenticationUtil;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.apache.pulsar.proxy.server.ProxyService;

/**
 * Kafka Protocol Handler load and run by Pulsar Service.
 */
@Slf4j
public class KafkaProtocolProxyMain {

    private final PulsarAdminProvider pulsarAdminProvider = new AuthenticatedPulsarAdminProvider();
    @Getter
    private KafkaServiceConfiguration kafkaConfig;
    private ProxyConfiguration proxyConfiguration;
    private KafkaSchemaRegistryProxyManager schemaRegistryProxyManager;
    private AuthenticationService authenticationService;
    private Function<String, String> brokerAddressMapper;
    private EventLoopGroup eventLoopGroupStandaloneMode;
    private PrometheusMetricsProvider statsProvider;
    private RequestStats requestStats;

    private final Function<String, String> defaultBrokerAddressMapper = (pulsarAddress -> {
        // The Mapping to the KOP port is done per-convention if you do not have access to Broker Discovery Service.
        String kafkaAddress = pulsarAddress
                .replace("pulsar://", "PLAINTEXT://")
                .replace("pulsar+ssl://", "SSL://");

        if (!StringUtils.isBlank(kafkaConfig.getKafkaProxyBrokerPortToKopMapping())) {
            String[] split = kafkaConfig.getKafkaProxyBrokerPortToKopMapping().split(",");
            for (String mapping : split) {
                String[] mappingSplit = mapping.split("=");
                if (mappingSplit.length == 2) {
                    kafkaAddress = kafkaAddress.replace(mappingSplit[0].trim(), mappingSplit[1].trim());
                }
            }
        } else {
            // standard mapping
            kafkaAddress = kafkaAddress
                    .replace("6650", "9092") // this is the standard case
                    .replace("6651", "9093")
                    .replace("6652", "9094")
                    .replace("6653", "9095");
        }
        return kafkaAddress;
    });

    public static void main(String... args) throws Exception {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                log.info("uncaughtException in thread {}", t, e);
            }
        });
        String configFile = args.length > 0 ? args[0] : "conf/kop_proxy.conf";
        KafkaProtocolProxyMain proxy = new KafkaProtocolProxyMain();
        ProxyConfiguration serviceConfiguration = PulsarConfigurationLoader.create(configFile,
                ProxyConfiguration.class);
        proxy.initialize(serviceConfiguration, null);
        proxy.startStandalone();
        log.info("Started");
        Thread.sleep(Integer.MAX_VALUE);
        proxy.close();
    }

    public void initialize(ProxyConfiguration conf, ProxyService proxyService) throws Exception {
        this.proxyConfiguration = conf;
        if (proxyService != null) {
            authenticationService = proxyService.getAuthenticationService();
            if (proxyService.getDiscoveryProvider() != null) {
                brokerAddressMapper = new BrokerAddressMapper(proxyService);
                log.info("Using Proxy DiscoveryProvider");
            } else {
                brokerAddressMapper = defaultBrokerAddressMapper;
                log.info("Using Broker address mapping by convention, "
                        + "because DiscoveryProvider is not configured (no zk configuration in the proxy)");
            }

        } else {
            authenticationService = new AuthenticationService(PulsarConfigurationLoader.convertFrom(conf));
            brokerAddressMapper = defaultBrokerAddressMapper;
            log.info("Using Broker address mapping by convention");
        }

        // init config
        kafkaConfig = ConfigurationUtils.create(conf.getProperties(), KafkaServiceConfiguration.class);

        // some of the configs value in conf.properties may not updated.
        // So need to get latest value from conf itself
        kafkaConfig.setAdvertisedAddress(conf.getAdvertisedAddress());
        kafkaConfig.setBindAddress(conf.getBindAddress());

        schemaRegistryProxyManager =
                new KafkaSchemaRegistryProxyManager(kafkaConfig, new SchemaRegistryBrokerProvider(),
                        () -> {
                            try {
                                return CompletableFuture.completedFuture(pulsarAdminProvider.getSuperUserPulsarAdmin());
                            } catch (PulsarClientException err) {
                                return FutureUtil.failedFuture(err);
                            }
                        }, authenticationService);

        // Validate the namespaces
        for (String fullNamespace : kafkaConfig.getKopAllowedNamespaces()) {
            final String[] tokens = fullNamespace.split("/");
            if (tokens.length != 2) {
                throw new IllegalArgumentException(
                        "Invalid namespace '" + fullNamespace + "' in kopAllowedNamespaces config");
            }
            NamespaceName.validateNamespaceName(
                    tokens[0].replace(KafkaServiceConfiguration.TENANT_PLACEHOLDER, kafkaConfig.getKafkaTenant()),
                    tokens[1].replace("*", kafkaConfig.getKafkaNamespace()));
        }

        log.info("AuthenticationEnabled:  {}", kafkaConfig.isAuthenticationEnabled());
        log.info("SaslAllowedMechanisms:  {}", kafkaConfig.getSaslAllowedMechanisms());
    }

    public void start() {
        log.info("Starting KafkaProtocolProxy, kop version is: '{}'", KopVersion.getVersion());
        log.info("Git Revision {}", KopVersion.getGitSha());
        log.info("Built by {} on {} at {}",
                KopVersion.getBuildUser(),
                KopVersion.getBuildHost(),
                KopVersion.getBuildTime());
        statsProvider = new PrometheusMetricsProvider();
        StatsLogger rootStatsLogger = statsProvider.getStatsLogger("");
        requestStats = new RequestStats(rootStatsLogger.scope(SERVER_SCOPE));
    }

    public void startStandalone() {

        eventLoopGroupStandaloneMode =
                EventLoopUtil.newEventLoopGroup(16, false, new DefaultThreadFactory("kop-broker-connection"));

        log.info("Starting KafkaProtocolProxy, kop version is: '{}'", KopVersion.getVersion());
        log.info("Git Revision {}", KopVersion.getGitSha());
        log.info("Built by {} on {} at {}",
                KopVersion.getBuildUser(),
                KopVersion.getBuildHost(),
                KopVersion.getBuildTime());
        newChannelInitializers().forEach((address, initializer) -> {
            System.out.println("Starting protocol at " + address);
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(eventLoopGroupStandaloneMode)
                    .channel(EventLoopUtil.getServerSocketChannelClass(eventLoopGroupStandaloneMode));
            bootstrap.childHandler(initializer);
            try {
                bootstrap.bind(address).sync();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void close() throws Exception {
        pulsarAdminProvider.close();
        if (eventLoopGroupStandaloneMode != null) {
            eventLoopGroupStandaloneMode.shutdownGracefully();
        }
        if (schemaRegistryProxyManager != null) {
            schemaRegistryProxyManager.close();
        }
    }

    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        checkState(kafkaConfig != null);

        try {
            ImmutableMap.Builder<InetSocketAddress, ChannelInitializer<SocketChannel>> builder =
                    ImmutableMap.builder();

            final Map<String, EndPoint> advertisedEndpointMap =
                    EndPoint.parseListeners(kafkaConfig.getKafkaAdvertisedListeners());
            EndPoint.parseListeners(kafkaConfig.getListeners()).forEach((protocol, endPoint) -> {
                EndPoint advertisedEndPoint = advertisedEndpointMap.get(protocol);
                if (advertisedEndPoint == null) {
                    // Use the bind endpoint as the advertised endpoint.
                    advertisedEndPoint = endPoint;
                }
                switch (endPoint.getSecurityProtocol()) {
                    case PLAINTEXT:
                    case SASL_PLAINTEXT:
                        builder.put(endPoint.getInetAddress(), new KafkaProxyChannelInitializer(pulsarAdminProvider,
                                authenticationService, kafkaConfig, false,
                                advertisedEndPoint, brokerAddressMapper, requestStats));
                        break;
                    case SSL:
                    case SASL_SSL:
                        builder.put(endPoint.getInetAddress(), new KafkaProxyChannelInitializer(pulsarAdminProvider,
                                authenticationService, kafkaConfig, true,
                                advertisedEndPoint, brokerAddressMapper, requestStats));
                        break;
                }
            });

            Optional<ChannelInitializer<SocketChannel>> schemaRegistryChannelInitializer =
                    schemaRegistryProxyManager.build();
            if (schemaRegistryChannelInitializer.isPresent()) {
                builder.put(schemaRegistryProxyManager.getAddress(), schemaRegistryChannelInitializer.get());
            }

            return builder.build();
        } catch (Exception e) {
            log.error("KafkaProtocolHandler newChannelInitializers failed with ", e);
            return null;
        }
    }

    public interface PulsarAdminProvider {
        PulsarAdmin getAdminForPrincipal(String originalPrincipal) throws PulsarClientException;

        void close();

        void invalidateAdminForPrincipal(String principal, PulsarAdmin expected, Throwable error);

        PulsarAdmin getSuperUserPulsarAdmin() throws PulsarClientException;
    }

    /**
     * This class allows the Proxy to inject the X-Original-Principal header.
     * It works only for Token Authentication.
     */
    private static class OriginalPrincipalAwareAuthentication implements Authentication {
        private final Authentication authentication;
        private final String originalPrincipal;

        public OriginalPrincipalAwareAuthentication(Authentication authentication, String originalPrincipal) {
            this.authentication = authentication;
            this.originalPrincipal = originalPrincipal;
        }

        @Override
        public String getAuthMethodName() {
            return authentication.getAuthMethodName();
        }

        @Override
        public void configure(Map<String, String> authParams) {
            authentication.configure(authParams);
        }

        @Override
        public void start() throws PulsarClientException {
            authentication.start();
        }

        @Override
        public void close() throws IOException {
            authentication.close();
        }

        @Override
        public AuthenticationDataProvider getAuthData() throws PulsarClientException {
            return authentication.getAuthData();
        }

        @Override
        public AuthenticationDataProvider getAuthData(String brokerHostName) throws PulsarClientException {
            return authentication.getAuthData(brokerHostName);
        }

        @Override
        public void authenticationStage(String requestUrl, AuthenticationDataProvider authData,
                                        Map<String, String> previousResHeaders,
                                        CompletableFuture<Map<String, String>> authFuture) {
            authentication.authenticationStage(requestUrl, authData, previousResHeaders, authFuture);
        }

        @Override
        public Set<Map.Entry<String, String>> newRequestHeader(String hostName, AuthenticationDataProvider authData,
                                                               Map<String, String> previousResHeaders)
                throws Exception {
            Set<Map.Entry<String, String>> res = authentication.newRequestHeader(hostName,
                    authData, previousResHeaders);
            if (originalPrincipal == null || originalPrincipal.isEmpty()) {
                return res;
            }
            HashSet<Map.Entry<String, String>> resWithPrincipal = new HashSet<>();
            if (res != null) {
                resWithPrincipal.addAll(res);
            }
            resWithPrincipal.add(new AbstractMap.SimpleImmutableEntry("X-Original-Principal", originalPrincipal));
            return resWithPrincipal;
        }
    }

    @AllArgsConstructor
    private final class SchemaRegistryBrokerProvider implements Supplier<String> {

        @Override
        public String get() {
            String rawServiceUrl = proxyConfiguration.getBrokerWebServiceURL();

            int colon = rawServiceUrl.lastIndexOf(':');
            String res;
            if (colon <= 0) {
                // no port ? this should not happen
                res = rawServiceUrl + ":" + kafkaConfig.getKopSchemaRegistryPort();
            } else {
                res = rawServiceUrl.substring(0, colon + 1) + kafkaConfig.getKopSchemaRegistryPort();
            }
            // no https to talk with the internal KOP broker
            res = res.replace("http://", "http://");
            log.info("SchemaRegistry mapping {} to {}", rawServiceUrl, res);
            return res;
        }
    }

    @AllArgsConstructor
    private final class BrokerAddressMapper implements Function<String, String> {
        private final ProxyService proxyService;
        private final ConcurrentHashMap<String, String> cache = new ConcurrentHashMap<>();

        @Override
        public String apply(String s) {
            return cache.computeIfAbsent(s, (address) -> {
                try {
                    List<? extends ServiceLookupData> availableBrokers = proxyService
                            .getDiscoveryProvider()
                            .getAvailableBrokers();
                    String mapped = availableBrokers
                            .stream()
                            .filter(data -> data.getPulsarServiceUrl().equals(address))
                            .map(data -> data.getProtocol("kafka"))
                            .findFirst()
                            .orElse(Optional.empty())
                            .orElse(null);
                    if (mapped != null) {
                        return mapped;
                    } else {
                        log.error("Cannot find KOP handler for broker {}, discovery info {}, using default mapping",
                                address, availableBrokers);
                        return defaultBrokerAddressMapper.apply(address);
                    }
                } catch (PulsarServerException err) {
                    throw new RuntimeException("Cannot find KOP handler for broker " + address, err);
                }
            });

        }
    }

    private class AuthenticatedPulsarAdminProvider implements PulsarAdminProvider {

        private final ConcurrentHashMap<String, PulsarAdmin> cache = new ConcurrentHashMap<>();

        @Override
        public void invalidateAdminForPrincipal(String originalPrincipal, PulsarAdmin expected, Throwable error) {
            if (originalPrincipal == null) {
                originalPrincipal = "";
            }
            cache.computeIfPresent(originalPrincipal, (p, current) -> {
                if (expected == current) {
                    // prevent race conditions and multiple threads that try to
                    // invalidate the same principal
                    current.close();
                    return null;
                } else {
                    return current;
                }
            });
        }

        public void close() {
            cache.values().forEach(admin -> {
                admin.close();
            });
        }

        @Override
        public PulsarAdmin getSuperUserPulsarAdmin() throws PulsarClientException {
            return getAdminForPrincipal(kafkaConfig.getKafkaProxySuperUserRole());
        }

        @Override
        public PulsarAdmin getAdminForPrincipal(String originalPrincipal) throws PulsarClientException {
            if (originalPrincipal == null) {
                originalPrincipal = "";
            }
            try {
                return cache.computeIfAbsent(originalPrincipal, principal -> {
                            try {
                                String auth = proxyConfiguration.getBrokerClientAuthenticationPlugin();
                                String authParams = proxyConfiguration.getBrokerClientAuthenticationParameters();

                                Authentication proxyAuthentication = AuthenticationUtil.create(auth, authParams);
                                Authentication authenticationWithPrincipal =
                                        new OriginalPrincipalAwareAuthentication(proxyAuthentication, principal);
                                return PulsarAdmin
                                        .builder()
                                        .authentication(authenticationWithPrincipal)
                                        .serviceHttpUrl(proxyConfiguration.getBrokerWebServiceURL())
                                        .allowTlsInsecureConnection(
                                                proxyConfiguration.isTlsAllowInsecureConnection())
                                        .enableTlsHostnameVerification(
                                                proxyConfiguration.isTlsHostnameVerificationEnabled())
                                        .readTimeout(5000, TimeUnit.MILLISECONDS)
                                        .connectionTimeout(5000, TimeUnit.MILLISECONDS)
                                        .build();
                            } catch (PulsarClientException err) {
                                throw new RuntimeException(err);
                            }
                        }
                );
            } catch (RuntimeException err) {
                if (err.getCause() instanceof PulsarClientException) {
                    throw (PulsarClientException) err.getCause();
                } else {
                    throw new PulsarClientException(err);
                }
            }
        }
    }
}
