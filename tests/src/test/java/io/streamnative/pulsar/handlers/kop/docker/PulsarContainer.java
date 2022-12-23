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
package io.streamnative.pulsar.handlers.kop.docker;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.MountableFile;

@Slf4j
public class PulsarContainer implements AutoCloseable {

  protected static final String PROTOCOLS_TEST_PROTOCOL_HANDLER_NAR = "/protocols/test-protocol-handler.nar";
  protected static final String PROXY_EXTENSION_TEST_NAR = "/proxyextensions/test-proxy-extension.nar";

  @Getter
  private GenericContainer<?> pulsarContainer;
  private GenericContainer<?> proxyContainer;
  private final Network network;
  private final boolean startProxy;
  private final String image;
  private final boolean enableTls;

  public PulsarContainer(Network network, boolean startProxy, String image,
                         boolean enableTls) {
    this.network = network;
    this.startProxy = startProxy;
    this.image = image;
    this.enableTls = enableTls;
  }

  public void start() throws Exception {
     String standaloneBaseCommand =
              "/pulsar/bin/apply-config-from-env.py /pulsar/conf/standalone.conf && bin/pulsar standalone"
                      + " --no-functions-worker -nss --advertised-address pulsar ";

      String proxyBaseCommand =
              "/pulsar/bin/apply-config-from-env.py /pulsar/conf/proxy.conf && bin/pulsar proxy";
    List<Integer> exposedPortsOnBroker = new ArrayList<>();
    exposedPortsOnBroker.add(8080);
    exposedPortsOnBroker.add(6650);
    exposedPortsOnBroker.add(9092);
    exposedPortsOnBroker.add(8001); // schema registry
    if (enableTls) {
        exposedPortsOnBroker.add(9093);
        exposedPortsOnBroker.add(8443);
        exposedPortsOnBroker.add(6651);
    }
    CountDownLatch pulsarReady = new CountDownLatch(1);
    pulsarContainer =
        new GenericContainer<>(image)
            .withNetwork(network)
            .withStartupTimeout(Duration.ofMinutes(1))
            .withNetworkAliases("pulsar")
                .withExposedPorts(exposedPortsOnBroker.toArray(new Integer[0])) // ensure that the ports are listening
            .withCopyFileToContainer(
                        MountableFile.forHostPath(getProtocolHandlerPath()),
                    "/pulsar/protocols/kop.nar")
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("ssl/docker/broker.cert.pem"),
                        "/pulsar/conf/broker.cert.pem")
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("ssl/docker/broker.key-pk8.pem"),
                        "/pulsar/conf/broker.key-pk8.pem")
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("ssl/docker/ca.cert.pem"),
                        "/pulsar/conf/ca.cert.pem")
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("ssl/docker/ca.jks"),
                        "/pulsar/conf/ca.jks")
                .withCommand("/bin/bash", "-c", standaloneBaseCommand)
            .withLogConsumer(
                (f) -> {
                  String text = f.getUtf8String().trim();
                  if (text.contains("messaging service is ready")) {
                    pulsarReady.countDown();
                  }
                  log.info(text);
                });
    pulsarContainer.withEnv("PULSAR_LOG_LEVEL", "info");
    pulsarContainer.withEnv("PULSAR_PREFIX_brokerClientAuthenticationPlugin",
            "org.apache.pulsar.client.impl.auth.AuthenticationToken");
    pulsarContainer.withEnv("PULSAR_PREFIX_brokerClientAuthenticationParameters",
            "{\"token\":\"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.Zjo9EQa8HdY1qFTgHxMVD4II7FKc1Q-dwNg_UOuGOvU\"");
    pulsarContainer.withEnv("PULSAR_PREFIX_brokerServiceURL", "pulsar://pulsar:6650");
    pulsarContainer.withEnv("PULSAR_PREFIX_brokerWebServiceURL", "http://pulsar:8080");
    pulsarContainer.withEnv("PULSAR_PREFIX_kafkaTransactionCoordinatorEnabled", "true");
    pulsarContainer.withEnv("PULSAR_PREFIX_narExtractionDirectory", "data");
    pulsarContainer.withEnv("PULSAR_PREFIX_brokerEntryMetadataInterceptors",
            "org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor,"
                    + "org.apache.pulsar.common.intercept.AppendBrokerTimestampMetadataInterceptor");
    pulsarContainer.withEnv("PULSAR_PREFIX_kafkaListeners", "PLAINTEXT://pulsar:9092");
    pulsarContainer.withEnv("PULSAR_PREFIX_kafkaAdvertisedListeners", "PLAINTEXT://pulsar:9092");
    pulsarContainer.withEnv("PULSAR_PREFIX_kopSchemaRegistryEnable", "true");
    pulsarContainer.withEnv("PULSAR_PREFIX_kopSchemaRegistryProxyPort", "8001");
    pulsarContainer.withEnv("PULSAR_PREFIX_messagingProtocols", "kafka");
    pulsarContainer.withEnv("PULSAR_PREFIX_protocolHandlerDirectory", "./protocols");
    pulsarContainer.withEnv("PULSAR_PREFIX_allowAutoTopicCreationType", "partitioned");

    pulsarContainer.withEnv("PULSAR_PREFIX_kafkaTxnProducerStateTopicNumPartitions", "1");
    pulsarContainer.withEnv("PULSAR_PREFIX_kafkaTxnProducerStateTopicSnapshotIntervalSeconds", "600");

    pulsarContainer.withEnv("PULSAR_PREFIX_authenticationEnabled", "false");
    pulsarContainer.withEnv("PULSAR_PREFIX_authorizationEnabled", "false");
    pulsarContainer.withEnv("PULSAR_PREFIX_authenticationProviders",
            "org.apache.pulsar.broker.authentication.AuthenticationProviderToken");
    pulsarContainer.withEnv("PULSAR_PREFIX_saslAllowedMechanisms", "PLAIN");
    pulsarContainer.withEnv("PULSAR_PREFIX_zookeeperServers", "pulsar:2181");
    pulsarContainer.withEnv("PULSAR_PREFIX_configurationStoreServers", "pulsar:2181");

    if (enableTls) {
        pulsarContainer.withEnv("PULSAR_PREFIX_kopSchemaRegistryEnableTls", "true");
        pulsarContainer.withEnv("PULSAR_PREFIX_kafkaListeners", "PLAINTEXT://pulsar:9092,SSL://pulsar:9093");
        pulsarContainer.withEnv("PULSAR_PREFIX_kafkaAdvertisedListeners", "PLAINTEXT://pulsar:9092,SSL://pulsar:9093");
        pulsarContainer.withEnv("PULSAR_PREFIX_tlsCertificateFilePath", "/pulsar/conf/broker.cert.pem");
        pulsarContainer.withEnv("PULSAR_PREFIX_tlsKeyFilePath", "/pulsar/conf/broker.key-pk8.pem");
        pulsarContainer.withEnv("PULSAR_PREFIX_tlsTrustCertsFilePath", "/pulsar/conf/ca.cert.pem");

        // for KOP broker to broker communications via TLS
        pulsarContainer.withEnv("PULSAR_PREFIX_kopSslTruststoreLocation", "/pulsar/conf/ca.jks");
        pulsarContainer.withEnv("PULSAR_PREFIX_kopSslTruststorePassword", "");

        pulsarContainer.withEnv("PULSAR_PREFIX_brokerServiceURLTLS", "pulsar+ssl://pulsar:6651");
        pulsarContainer.withEnv("PULSAR_PREFIX_brokerWebServiceURLTLS", "https://pulsar:8443");
        pulsarContainer.withEnv("PULSAR_PREFIX_brokerServicePortTls", "6651");
        pulsarContainer.withEnv("PULSAR_PREFIX_webServicePortTls", "8443");

        pulsarContainer.withEnv("PULSAR_PREFIX_tlsAllowInsecureConnection", "true");
        pulsarContainer.withEnv("PULSAR_PREFIX_tlsHostnameVerificationEnabled", "false");

        pulsarContainer.withEnv("PULSAR_PREFIX_kopTlsEnabledWithBroker", "true");
        pulsarContainer.withEnv("PULSAR_PREFIX_tlsEnabledWithBroker", "true");
        pulsarContainer.withEnv("PULSAR_PREFIX_brokerServiceURLTLS", "pulsar+ssl://pulsar:6651");
        pulsarContainer.withEnv("PULSAR_PREFIX_brokerWebServiceURLTLS", "https://pulsar:8443");
    }
    pulsarContainer.start();
    assertTrue(pulsarReady.await(1, TimeUnit.MINUTES));

    if (startProxy) {
        List<Integer> exposedPortsOnProxy = new ArrayList<>();
        exposedPortsOnProxy.add(8080);
        exposedPortsOnProxy.add(9092);
        exposedPortsOnProxy.add(8081); // schema registry
        exposedPortsOnProxy.add(6650);
        if (enableTls) {
            exposedPortsOnProxy.add(9093);
            exposedPortsOnProxy.add(8443);
            exposedPortsOnProxy.add(6651);
        }
        CountDownLatch proxyReady = new CountDownLatch(1);
        proxyContainer =
                new GenericContainer<>(image)
                        .withNetwork(network)
                        .withStartupTimeout(Duration.ofMinutes(1))
                        .withNetworkAliases("pulsarproxy")
                        .withExposedPorts(exposedPortsOnProxy.toArray(new Integer[0]))
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(getProxyExtensionPath()),
                                "/pulsar/proxyextensions/kop.nar")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("ssl/docker/proxy.cert.pem"),
                                "/pulsar/conf/proxy.cert.pem")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("ssl/docker/proxy.key-pk8.pem"),
                                "/pulsar/conf/proxy.key-pk8.pem")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("ssl/docker/ca.cert.pem"),
                                "/pulsar/conf/ca.cert.pem")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("ssl/docker/ca.jks"),
                                "/pulsar/conf/ca.jks")

                        .withCommand("/bin/bash", "-c", proxyBaseCommand)
                        .withLogConsumer(
                                (f) -> {
                                    String text = f.getUtf8String().trim();
                                    if (text.contains("Server started at end point")) {
                                        proxyReady.countDown();
                                    }
                                    log.info(text);
                                });
        proxyContainer.withEnv("JAVA_JDK_OPTIONS", "-Djdk.tls.disabledAlgorithms=");
        proxyContainer.withEnv("PULSAR_LOG_LEVEL", "info");
        proxyContainer.withEnv("PULSAR_PREFIX_brokerServiceURL", "pulsar://pulsar:6650");
        proxyContainer.withEnv("PULSAR_PREFIX_brokerWebServiceURL", "http://pulsar:8080");
        proxyContainer.withEnv("PULSAR_PREFIX_brokerClientAuthenticationPlugin",
                "org.apache.pulsar.client.impl.auth.AuthenticationToken");
        proxyContainer.withEnv("PULSAR_PREFIX_brokerClientAuthenticationParameters",
                "{\"token\":\"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.Zjo9EQa8HdY1qFTgHxMVD4II7FKc1Q-dwNg_UOuGOvU\"");

        proxyContainer.withEnv("PULSAR_PREFIX_narExtractionDirectory", "data");
        proxyContainer.withEnv("PULSAR_PREFIX_kafkaListeners", "PLAINTEXT://pulsarproxy:9092");
        proxyContainer.withEnv("PULSAR_PREFIX_kafkaAdvertisedListeners", "PLAINTEXT://pulsarproxy:9092");
        proxyContainer.withEnv("PULSAR_PREFIX_kopSchemaRegistryEnable", "true");
        proxyContainer.withEnv("PULSAR_PREFIX_kopSchemaRegistryProxyPort", "8081");
        proxyContainer.withEnv("PULSAR_PREFIX_proxyExtensions", "kafka");
        proxyContainer.withEnv("PULSAR_PREFIX_proxyExtensionsDirectory", "./proxyextensions");

        proxyContainer.withEnv("PULSAR_PREFIX_authenticationEnabled", "false");
        proxyContainer.withEnv("PULSAR_PREFIX_authorizationEnabled", "false");
        proxyContainer.withEnv("PULSAR_PREFIX_authenticationProviders",
                "org.apache.pulsar.broker.authentication.AuthenticationProviderToken");
        proxyContainer.withEnv("PULSAR_PREFIX_saslAllowedMechanisms", "PLAIN");
        proxyContainer.withEnv("PULSAR_PREFIX_zookeeperServers", "pulsar:2181");
        proxyContainer.withEnv("PULSAR_PREFIX_configurationStoreServers", "pulsar:2181");

        // it is valid to set this URLS even if the broker does not enable TLS
        proxyContainer.withEnv("PULSAR_PREFIX_brokerServiceURLTLS", "pulsar+ssl://pulsar:6651");
        proxyContainer.withEnv("PULSAR_PREFIX_brokerWebServiceURLTLS", "https://pulsar:8443");

        if (enableTls) {
            proxyContainer.withEnv("PULSAR_PREFIX_kopSchemaRegistryProxyEnableTls", "true");
            proxyContainer.withEnv("PULSAR_PREFIX_kafkaListeners",
                    "PLAINTEXT://pulsarproxy:9092,SSL://pulsarproxy:9093");
            proxyContainer.withEnv("PULSAR_PREFIX_kafkaAdvertisedListeners",
                    "PLAINTEXT://pulsarproxy:9092,SSL://pulsarproxy:9093");

            proxyContainer.withEnv("PULSAR_PREFIX_servicePortTls", "6651");
            proxyContainer.withEnv("PULSAR_PREFIX_webServicePortTls", "8443");
            proxyContainer.withEnv("PULSAR_PREFIX_tlsCertificateFilePath", "/pulsar/conf/proxy.cert.pem");
            proxyContainer.withEnv("PULSAR_PREFIX_tlsKeyFilePath", "/pulsar/conf/proxy.key-pk8.pem");
            proxyContainer.withEnv("PULSAR_PREFIX_tlsTrustCertsFilePath", "/pulsar/conf/ca.cert.pem");

            // Proxy to broker communication
            proxyContainer.withEnv("PULSAR_PREFIX_kopTlsEnabledWithBroker", "true");
            proxyContainer.withEnv("PULSAR_PREFIX_tlsEnabledWithBroker", "true");
            proxyContainer.withEnv("PULSAR_PREFIX_kopSslTruststoreLocation", "/pulsar/conf/ca.jks");
            proxyContainer.withEnv("PULSAR_PREFIX_kopSslTruststorePassword", "pulsar");
            proxyContainer.withEnv("PULSAR_PREFIX_tlsAllowInsecureConnection", "true");
            proxyContainer.withEnv("PULSAR_PREFIX_tlsHostnameVerificationEnabled", "false");
        }

        proxyContainer.start();
        assertTrue(proxyReady.await(1, TimeUnit.MINUTES));
    }

  }

  @Override
  public void close() {
    if (proxyContainer != null) {
      proxyContainer.stop();
    }
    if (pulsarContainer != null) {
      pulsarContainer.stop();
    }
  }

  protected Path getProtocolHandlerPath() {
        URL testHandlerUrl = this.getClass().getResource(PROTOCOLS_TEST_PROTOCOL_HANDLER_NAR);
        Path handlerPath;
        try {
            if (testHandlerUrl == null) {
                throw new RuntimeException("Cannot find " + PROTOCOLS_TEST_PROTOCOL_HANDLER_NAR);
            }
            handlerPath = Paths.get(testHandlerUrl.toURI());
        } catch (Exception e) {
            log.error("failed to get handler Path, handlerUrl: {}. Exception: ", testHandlerUrl, e);
            throw new RuntimeException(e);
        }
        Path res = handlerPath.toFile().toPath();
        log.info("Loading NAR file from {}", res.toAbsolutePath());
        return res;
    }

    protected Path getProxyExtensionPath() {
        URL testHandlerUrl = this.getClass().getResource(PROXY_EXTENSION_TEST_NAR);
        Path handlerPath;
        try {
            if (testHandlerUrl == null) {
                throw new RuntimeException("Cannot find " + PROXY_EXTENSION_TEST_NAR);
            }
            handlerPath = Paths.get(testHandlerUrl.toURI());
        } catch (Exception e) {
            log.error("failed to get extensions Path, handlerUrl: {}. Exception: ", testHandlerUrl, e);
            throw new RuntimeException(e);
        }
        Path res = handlerPath.toFile().toPath();
        log.info("Loading NAR file from {}", res.toAbsolutePath());
        return res;
    }
}
