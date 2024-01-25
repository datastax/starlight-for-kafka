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

import static org.testng.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.MountableFile;
import org.testng.annotations.Test;

@Slf4j
public class DockerTest {

    private static final String IMAGE_LUNASTREAMING31 = "datastax/lunastreaming:3.1";
    private static final String IMAGE_PULSAR31 = "apachepulsar/pulsar:3.1.1";
    private static final String CONFLUENT_CLIENT = "confluentinc/cp-kafka:latest";
    private static final String CONFLUENT_SCHEMAREGISTRY_CLIENT = "confluentinc/cp-schema-registry:latest";

    @Test
    public void test() throws Exception {
        test("pulsar:9092", false, IMAGE_PULSAR31);
    }

    @Test
    public void testProxy() throws Exception {
        test("pulsarproxy:9092", true, IMAGE_PULSAR31);
    }

    @Test
    public void testAvro() throws Exception {
        testAvro("pulsar:9092", "http://pulsar:8001", false, IMAGE_PULSAR31);
    }

    @Test
    public void testAvroProxy() throws Exception {
        testAvro("pulsarproxy:9092", "http://pulsarproxy:8081", true, IMAGE_PULSAR31);
    }

//    @Test
    public void testLunaStreaming() throws Exception {
        test("pulsar:9092", false, IMAGE_LUNASTREAMING31);
    }

//    @Test
    public void testLunaStreamingTls() throws Exception {
        test("pulsar:9093", false, IMAGE_LUNASTREAMING31, true);
    }

//    @Test
    public void testProxyLunaStreaming() throws Exception {
        test("pulsarproxy:9092", true, IMAGE_LUNASTREAMING31);
    }

//    @Test
    public void testProxyLunaStreamingTls() throws Exception {
        test("pulsarproxy:9093", true, IMAGE_LUNASTREAMING31, true);
    }

//    @Test
    public void testAvroLunaStreaming() throws Exception {
        testAvro("pulsar:9092", "http://pulsar:8001", false, IMAGE_LUNASTREAMING31);
    }

//    @Test
    public void testAvroLunaStreamingTls() throws Exception {
        testAvro("pulsar:9093", "https://pulsar:8001", false, IMAGE_LUNASTREAMING31, true);
    }

//    @Test
    public void testAvroProxyLunaStreaming() throws Exception {
        testAvro("pulsarproxy:9092", "http://pulsarproxy:8081", true, IMAGE_LUNASTREAMING31);
    }

//    @Test
    public void testAvroProxyLunaStreamingTls() throws Exception {
        testAvro("pulsarproxy:9093", "https://pulsarproxy:8081", true, IMAGE_LUNASTREAMING31, true);
    }

    private void test(String kafkaAddress, boolean proxy, String image) throws Exception {
        test(kafkaAddress, proxy, image, false);
    }

    private void test(String kafkaAddress, boolean proxy, String image, boolean tls) throws Exception {
        // create a docker network
        try (Network network = Network.newNetwork();) {
            // start Pulsar and wait for it to be ready to accept requests
            try (PulsarContainer pulsarContainer = new PulsarContainer(network, proxy, image, tls);) {
                pulsarContainer.start();

                String consumerConfigurationTls = "";
                String producerConfigurationTls = "";
                if (tls) {
                    consumerConfigurationTls = "--consumer.config /home/appuser/client.properties";
                    producerConfigurationTls = "--producer.config /home/appuser/client.properties";
                }
                CountDownLatch received = new CountDownLatch(1);
                try (GenericContainer clientContainer = new GenericContainer(CONFLUENT_CLIENT)
                        .withNetwork(network)
                        .withCommand("bash", "-c", "kafka-console-consumer --bootstrap-server " + kafkaAddress
                                + " --topic test --from-beginning " + consumerConfigurationTls)
                        .withLogConsumer(new Consumer<OutputFrame>() {
                            @Override
                            public void accept(OutputFrame outputFrame) {
                                log.info("CONSUMER > {}", outputFrame.getUtf8String());
                                if (outputFrame.getUtf8String().contains("This-is-my-message")) {
                                    received.countDown();
                                }
                            }
                        })) {
                    if (tls) {
                        clientContainer.withCopyFileToContainer(
                                MountableFile.forClasspathResource("ssl/docker/kafka_client_config_tls.properties"),
                                "/home/appuser/client.properties");
                        clientContainer.withCopyFileToContainer(
                                MountableFile.forClasspathResource("ssl/docker/ca.jks"),
                                "/home/appuser/ca.jks");
                    }
                    clientContainer.start();

                    CountDownLatch sent = new CountDownLatch(1);
                    try (GenericContainer producerContainer = new GenericContainer(CONFLUENT_CLIENT)
                            .withNetwork(network)
                            .withCommand("bash", "-c",
                                    "echo This-is-my-message > file.txt && "
                                            + "kafka-console-producer --bootstrap-server " + kafkaAddress
                                            + "  --topic test " + producerConfigurationTls + " < file.txt && "
                                            + "echo FINISHEDPRODUCER")
                            .withLogConsumer(new Consumer<OutputFrame>() {
                                @Override
                                public void accept(OutputFrame outputFrame) {
                                    log.info("PRODUCER > {}", outputFrame.getUtf8String());
                                    if (outputFrame.getUtf8String().contains("FINISHEDPRODUCER")) {
                                        sent.countDown();
                                    }
                                }
                            })) {
                        if (tls) {
                            producerContainer.withCopyFileToContainer(
                                    MountableFile.forClasspathResource("ssl/docker/kafka_client_config_tls.properties"),
                                    "/home/appuser/client.properties");
                            producerContainer.withCopyFileToContainer(
                                    MountableFile.forClasspathResource("ssl/docker/ca.jks"),
                                    "/home/appuser/ca.jks");
                        }
                        producerContainer.start();
                        assertTrue(sent.await(5, TimeUnit.MINUTES));
                    }

                    assertTrue(received.await(5, TimeUnit.MINUTES));
                }
            }
        }
    }

    private void testAvro(String kafkaAddress, String registryAddress, boolean proxy, String image) throws Exception {
        testAvro(kafkaAddress, registryAddress, proxy, image, false);
    }

    private void testAvro(String kafkaAddress, String registryAddress, boolean proxy,
                          String image, boolean tls) throws Exception {
        String consumerConfigurationTls = "";
        String producerConfigurationTls = "";
        if (tls) {
            consumerConfigurationTls = "--consumer.config /home/appuser/client.properties";
            producerConfigurationTls = "--producer.config /home/appuser/client.properties";
        }

        // create a docker network
        try (Network network = Network.newNetwork();) {
            // start Pulsar and wait for it to be ready to accept requests
            try (PulsarContainer pulsarContainer = new PulsarContainer(network, proxy, image, tls);) {
                pulsarContainer.start();

                CountDownLatch received = new CountDownLatch(1);
                try (GenericContainer clientContainer = new GenericContainer(CONFLUENT_SCHEMAREGISTRY_CLIENT)
                        .withNetwork(network)
                        .withCommand("bash", "-c", "kafka-avro-console-consumer --bootstrap-server " + kafkaAddress
                                + " --topic test --from-beginning --property schema.registry.url=" + registryAddress
                                + " " + consumerConfigurationTls)
                        .withLogConsumer(new Consumer<OutputFrame>() {
                            @Override
                            public void accept(OutputFrame outputFrame) {
                                log.info("CONSUMER > {}", outputFrame.getUtf8String());
                                if (outputFrame.getUtf8String().contains("Pennsylvania")) {
                                    received.countDown();
                                }
                            }
                        })) {
                    if (tls) {
                        clientContainer.withCopyFileToContainer(
                                MountableFile.forClasspathResource("ssl/docker/kafka_client_config_tls.properties"),
                                "/home/appuser/client.properties");
                        clientContainer.withCopyFileToContainer(
                                MountableFile.forClasspathResource("ssl/docker/ca.jks"),
                                "/home/appuser/ca.jks");
                        clientContainer.withEnv("SCHEMA_REGISTRY_OPTS",
                                "-Djavax.net.ssl.trustStore=/home/appuser/ca.jks "
                                + "-Djavax.net.ssl.trustStorePassword=pulsar"
                        );
                    }
                    clientContainer.start();

                    // sample taken from https://kafka-tutorials.confluent.io/kafka-console-consumer-producer/kafka.html
                    CountDownLatch sent = new CountDownLatch(1);
                    try (GenericContainer producerContainer = new GenericContainer(
                            CONFLUENT_SCHEMAREGISTRY_CLIENT)
                            .withNetwork(network)
                            .withCommand("bash", "-c",
                                    "echo '{\"number\": 2343439, \"date\": 1596501510, "
                                            + "\"shipping_address\": \"1600 Pennsylvania Avenue NW, Washington, DC "
                                            + "20500, USA\", \"subtotal\": 1000.0, \"shipping_cost\": 20.0, \"tax\": "
                                            + "0.00}' > file.txt && "
                                            + "echo '{"
                                            + "  \"type\": \"record\","
                                            + "  \"namespace\": \"io.confluent.tutorial.pojo.avro\","
                                            + "  \"name\": \"OrderDetail\","
                                            + "  \"fields\": ["
                                            + "    {"
                                            + "      \"name\": \"number\","
                                            + "      \"type\": \"long\""
                                            + "    },"
                                            + "    {"
                                            + "      \"name\": \"date\","
                                            + "      \"type\": \"long\","
                                            + "      \"logicalType\": \"date\""
                                            + "    },"
                                            + "    {"
                                            + "      \"name\": \"shipping_address\","
                                            + "      \"type\": \"string\""
                                            + "    },"
                                            + "    {"
                                            + "      \"name\": \"subtotal\","
                                            + "      \"type\": \"double\""
                                            + "    },"
                                            + "    {"
                                            + "      \"name\": \"shipping_cost\", "
                                            + "      \"type\": \"double\""
                                            + "    },"
                                            + "    {"
                                            + "      \"name\":\"tax\","
                                            + "      \"type\":\"double\""
                                            + "    }"
                                            + "  ]"
                                            + "}' > order_detail.avsc && "
                                            + " kafka-avro-console-producer --bootstrap-server " + kafkaAddress
                                            + " --topic test --property schema.registry.url=" + registryAddress
                                            + " --property value.schema=\"$(< order_detail.avsc)\" < file.txt "
                                            + " " + producerConfigurationTls + " && "
                                            + "echo FINISHEDPRODUCER")
                            .withLogConsumer(new Consumer<OutputFrame>() {
                                @Override
                                public void accept(OutputFrame outputFrame) {
                                    log.info("PRODUCER > {}", outputFrame.getUtf8String());
                                    if (outputFrame.getUtf8String().contains("FINISHEDPRODUCER")) {
                                        sent.countDown();
                                    }
                                }
                            })) {
                        if (tls) {
                            producerContainer.withCopyFileToContainer(
                                    MountableFile.forClasspathResource("ssl/docker/kafka_client_config_tls.properties"),
                                    "/home/appuser/client.properties");
                            producerContainer.withCopyFileToContainer(
                                    MountableFile.forClasspathResource("ssl/docker/ca.jks"),
                                    "/home/appuser/ca.jks");
                            producerContainer.withEnv("SCHEMA_REGISTRY_OPTS",
                                    "-Djavax.net.ssl.trustStore=/home/appuser/ca.jks "
                                            + "-Djavax.net.ssl.trustStorePassword=pulsar"
                            );
                        }
                        producerContainer.start();
                        assertTrue(sent.await(5, TimeUnit.MINUTES));
                    }

                    assertTrue(received.await(5, TimeUnit.MINUTES));
                }
            }
        }
    }

}
