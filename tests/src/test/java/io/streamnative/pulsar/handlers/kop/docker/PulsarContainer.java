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

  public PulsarContainer(Network network, boolean startProxy, String image) {
    this.network = network;
    this.startProxy = startProxy;
    this.image = image;
  }

  public void start() throws Exception {
    CountDownLatch pulsarReady = new CountDownLatch(1);
    pulsarContainer =
        new GenericContainer<>(image)
            .withNetwork(network)
            .withNetworkAliases("pulsar")
                .withExposedPorts(8080, 9092, 8001) // ensure that the ports are listening
            .withCopyFileToContainer(
                        MountableFile.forHostPath(getProtocolHandlerPath()),
                    "/pulsar/protocols/kop.nar")
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("standalone_with_kop.conf"),
                        "/pulsar/conf/standalone.conf")
            .withCommand(
                "bin/pulsar",
                "standalone",
                "--advertised-address",
                "pulsar",
                "--no-functions-worker",
                "-nss")
            .withLogConsumer(
                (f) -> {
                  String text = f.getUtf8String().trim();
                  if (text.contains("messaging service is ready")) {
                    pulsarReady.countDown();
                  }
                  log.info(text);
                });
    pulsarContainer.start();
    assertTrue(pulsarReady.await(1, TimeUnit.MINUTES));

    if (startProxy) {
        CountDownLatch proxyReady = new CountDownLatch(1);
        proxyContainer =
                new GenericContainer<>(image)
                        .withNetwork(network)
                        .withNetworkAliases("pulsarproxy")
                        .withExposedPorts(8089, 9092, 8081, 9093) // ensure that the ports are listening
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(getProxyExtensionPath()),
                                "/pulsar/proxyextensions/kop.nar")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("proxy_with_kop.conf"),
                                "/pulsar/conf/proxy.conf")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("ssl/proxy/proxy.cert.pem"),
                                "/pulsar/conf/proxy.cert.pem")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("ssl/proxy/proxy.key-pk8.pem"),
                                "/pulsar/conf/proxy.key-pk8.pem")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("ssl/proxy/ca.cert.pem"),
                                "/pulsar/conf/ca.cert.pem")

                        .withCommand(
                                "bin/pulsar",
                                "proxy")
                        .withLogConsumer(
                                (f) -> {
                                    String text = f.getUtf8String().trim();
                                    if (text.contains("Server started at end point")) {
                                        proxyReady.countDown();
                                    }
                                    log.info(text);
                                });
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
