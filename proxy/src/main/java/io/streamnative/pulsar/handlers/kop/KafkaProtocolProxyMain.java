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
import com.google.common.collect.ImmutableMap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.streamnative.pulsar.handlers.kop.utils.ConfigurationUtils;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.AuthenticationUtil;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.proxy.server.ProxyService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;

/**
 * Kafka Protocol Handler load and run by Pulsar Service.
 */
@Slf4j
public class KafkaProtocolProxyMain {

    @Getter
    private KafkaServiceConfiguration kafkaConfig;
    private ProxyConfiguration proxyConfiguration;

    private final PulsarAdminProvider pulsarAdminProvider = new AuthenticatedPulsarAdminProvider();
    private AuthenticationService authenticationService;
    private Function<String, String> brokerAddressMapper;

    @AllArgsConstructor
    private static final class BrokerAddressMapper implements Function<String, String> {
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
                        log.error("Cannot find KOP handler for broker {}, discovery info {}", address, availableBrokers);
                        throw new RuntimeException("Cannot find KOP handler for broker " + address);
                    }
                } catch (PulsarServerException err) {
                    throw new RuntimeException("Cannot find KOP handler for broker " + address, err);
                }
            });

        }
    }

    public void initialize(ProxyConfiguration conf, ProxyService proxyService) throws Exception {
        this.proxyConfiguration = conf;
        if (proxyService != null) {
            authenticationService = proxyService.getAuthenticationService();
            if (proxyService.getDiscoveryProvider() != null) {
                brokerAddressMapper = new BrokerAddressMapper(proxyService);
                log.info("Using Proxy DiscoveryProvider");
            } else {
                brokerAddressMapper = null;
                log.info("Using Broker address mapping by convention, because DiscoveryProvider is not configured (no zk configuration in the proxy)");
            }

        } else {
            authenticationService = new AuthenticationService(PulsarConfigurationLoader.convertFrom(conf));
            brokerAddressMapper = null;
            log.info("Using Broker address mapping by convention");
        }

        // init config
        kafkaConfig = ConfigurationUtils.create(conf.getProperties(), KafkaServiceConfiguration.class);

        // some of the configs value in conf.properties may not updated.
        // So need to get latest value from conf itself
        kafkaConfig.setAdvertisedAddress(conf.getAdvertisedAddress());
        kafkaConfig.setBindAddress(conf.getBindAddress());

        KopTopic.initialize(kafkaConfig.getKafkaTenant() + "/" + kafkaConfig.getKafkaNamespace());

        // Validate the namespaces
        for (String fullNamespace : kafkaConfig.getKopAllowedNamespaces()) {
            final String[] tokens = fullNamespace.split("/");
            if (tokens.length != 2) {
                throw new IllegalArgumentException(
                        "Invalid namespace '" + fullNamespace + "' in kopAllowedNamespaces config");
            }
            NamespaceName.validateNamespaceName(tokens[0], tokens[1]);
        }


        log.info("AuthenticationEnabled:  {}", kafkaConfig.isAuthenticationEnabled());
        log.info("SaslAllowedMechanisms:  {}", kafkaConfig.getSaslAllowedMechanisms());
    }

    public static void main(String ... args) throws Exception {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                log.info("uncaughtException in thread {}",t, e);
            }
        });
        String configFile = args.length > 0 ? args[0] : "conf/kop_proxy.conf";
        KafkaProtocolProxyMain proxy = new KafkaProtocolProxyMain();
        ProxyConfiguration serviceConfiguration = PulsarConfigurationLoader.create(configFile, ProxyConfiguration.class);
        proxy.initialize(serviceConfiguration, null);
        proxy.startStandalone();
        log.info("Started");
        Thread.sleep(Integer.MAX_VALUE);
        proxy.close();
    }

    public void start() {
        log.info("Starting KafkaProtocolProxy, kop version is: '{}'", KopVersion.getVersion());
        log.info("Git Revision {}", KopVersion.getGitSha());
        log.info("Built by {} on {} at {}",
                KopVersion.getBuildUser(),
                KopVersion.getBuildHost(),
                KopVersion.getBuildTime());
    }

    public void startStandalone() {

        log.info("Starting KafkaProtocolProxy, kop version is: '{}'", KopVersion.getVersion());
        log.info("Git Revision {}", KopVersion.getGitSha());
        log.info("Built by {} on {} at {}",
            KopVersion.getBuildUser(),
            KopVersion.getBuildHost(),
            KopVersion.getBuildTime());
        newChannelInitializers().forEach( (address, initializer) ->{
            System.out.println("Starting protocol at "+address);
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(new NioEventLoopGroup())
                    .channel(NioServerSocketChannel.class);
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
    }

    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        checkState(kafkaConfig != null);

        try {
            ImmutableMap.Builder<InetSocketAddress, ChannelInitializer<SocketChannel>> builder =
                ImmutableMap.builder();

            final Map<SecurityProtocol, EndPoint> advertisedEndpointMap =
                    EndPoint.parseListeners(kafkaConfig.getKafkaAdvertisedListeners());
            EndPoint.parseListeners(kafkaConfig.getListeners()).forEach((protocol, endPoint) -> {
                EndPoint advertisedEndPoint = advertisedEndpointMap.get(protocol);
                if (advertisedEndPoint == null) {
                    // Use the bind endpoint as the advertised endpoint.
                    advertisedEndPoint = endPoint;
                }
                switch (protocol) {
                    case PLAINTEXT:
                    case SASL_PLAINTEXT:
                        builder.put(endPoint.getInetAddress(), new KafkaProxyChannelInitializer(pulsarAdminProvider,
                                authenticationService, kafkaConfig, false,  advertisedEndPoint, brokerAddressMapper));
                        break;
                    case SSL:
                    case SASL_SSL:
                        builder.put(endPoint.getInetAddress(), new KafkaProxyChannelInitializer(pulsarAdminProvider,
                                authenticationService, kafkaConfig, true, advertisedEndPoint, brokerAddressMapper));
                        break;
                }
            });

            return builder.build();
        } catch (Exception e){
            log.error("KafkaProtocolHandler newChannelInitializers failed with ", e);
            return null;
        }
    }

    public interface PulsarAdminProvider {
        PulsarAdmin getAdminForPrincipal(String originalPrincipal) throws PulsarClientException;
        void close();
    }

    private class AuthenticatedPulsarAdminProvider implements PulsarAdminProvider {

        private final ConcurrentHashMap<String, PulsarAdmin> cache = new ConcurrentHashMap<>();

        public void close() {
            cache.values().forEach(admin -> {
                admin.close();
            });
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
                            Authentication authenticationWithPrincipal = new OriginalPrincipalAwareAuthentication(proxyAuthentication, principal);

                            return PulsarAdmin
                                    .builder()
                                    .authentication(authenticationWithPrincipal)
                                    .serviceHttpUrl(proxyConfiguration.getBrokerWebServiceURL())
                                    .allowTlsInsecureConnection(proxyConfiguration.isTlsAllowInsecureConnection())
                                    .enableTlsHostnameVerification(proxyConfiguration.isTlsHostnameVerificationEnabled())
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
        public void authenticationStage(String requestUrl, AuthenticationDataProvider authData, Map<String, String> previousResHeaders, CompletableFuture<Map<String, String>> authFuture) {
            authentication.authenticationStage(requestUrl, authData, previousResHeaders, authFuture);
        }

        @Override
        public Set<Map.Entry<String, String>> newRequestHeader(String hostName, AuthenticationDataProvider authData, Map<String, String> previousResHeaders) throws Exception {
            Set<Map.Entry<String, String>> res =  authentication.newRequestHeader(hostName, authData, previousResHeaders);
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
}
