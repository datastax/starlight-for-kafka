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
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

@Slf4j
public class DockerTest {

    private static final String IMAGE_LS280 = "datastax/lunastreaming:2.8.0_1.1.40";
    private static final String IMAGE_LS283 = "datastax/lunastreaming:2.8.3_1.0.7";
    private static final String IMAGE_PULSAR210 = "apachepulsar/pulsar:2.10.0";

    @Test
    public void test() throws Exception {
        test("pulsar:9092", false);
    }

    @Test
    public void testProxy() throws Exception {
        test("pulsarproxy:9092", true);
    }

    @Test
    public void testAvro() throws Exception {
        testAvro("pulsar:9092", "http://pulsar:8001", false);
    }

    @Test
    public void testAvroProxy() throws Exception {
        testAvro("pulsarproxy:9092", "http://pulsarproxy:8081", true);
    }

    private void test(String kafkaAddress, boolean proxy) throws Exception {
        test(kafkaAddress, proxy, IMAGE_PULSAR210);
    }

    private void test(String kafkaAddress, boolean proxy, String image) throws Exception {
        // create a docker network
        try (Network network = Network.newNetwork();) {
            // start Pulsar and wait for it to be ready to accept requests
            try (PulsarContainer pulsarContainer = new PulsarContainer(network, proxy, image);) {
                pulsarContainer.start();

                CountDownLatch received = new CountDownLatch(1);
                try (GenericContainer clientContainer = new GenericContainer("confluentinc/cp-kafka:latest")
                        .withNetwork(network)
                        .withCommand("bash", "-c", "kafka-console-consumer --bootstrap-server " + kafkaAddress
                                + " --topic test --from-beginning")
                        .withLogConsumer(new Consumer<OutputFrame>() {
                            @Override
                            public void accept(OutputFrame outputFrame) {
                                log.info("CONSUMER > {}", outputFrame.getUtf8String());
                                if (outputFrame.getUtf8String().contains("This-is-my-message")) {
                                    received.countDown();
                                }
                            }
                        })) {
                    clientContainer.start();

                    CountDownLatch sent = new CountDownLatch(1);
                    try (GenericContainer producerContainer = new GenericContainer("confluentinc/cp-kafka:latest")
                            .withNetwork(network)
                            .withCommand("bash", "-c",
                                    "echo This-is-my-message > file.txt && "
                                            + "kafka-console-producer --bootstrap-server " + kafkaAddress
                                            + "  --topic test < file.txt && "
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
                        producerContainer.start();
                        assertTrue(sent.await(60, TimeUnit.SECONDS));
                    }

                    assertTrue(received.await(60, TimeUnit.SECONDS));
                }
            }
        }
    }

    private void testAvro(String kafkaAddress, String registryAddress, boolean proxy) throws Exception {
        // create a docker network
        try (Network network = Network.newNetwork();) {
            // start Pulsar and wait for it to be ready to accept requests
            try (PulsarContainer pulsarContainer = new PulsarContainer(network, proxy, IMAGE_LS280);) {
                pulsarContainer.start();

                CountDownLatch received = new CountDownLatch(1);
                try (GenericContainer clientContainer = new GenericContainer("confluentinc/cp-schema-registry:latest")
                        .withNetwork(network)
                        .withCommand("bash", "-c", "kafka-avro-console-consumer --bootstrap-server " + kafkaAddress
                                + " --topic test --from-beginning --property schema.registry.url=" + registryAddress)
                        .withLogConsumer(new Consumer<OutputFrame>() {
                            @Override
                            public void accept(OutputFrame outputFrame) {
                                log.info("CONSUMER > {}", outputFrame.getUtf8String());
                                if (outputFrame.getUtf8String().contains("Pennsylvania")) {
                                    received.countDown();
                                }
                            }
                        })) {
                    clientContainer.start();

                    // sample taken from https://kafka-tutorials.confluent.io/kafka-console-consumer-producer/kafka.html
                    CountDownLatch sent = new CountDownLatch(1);
                    try (GenericContainer producerContainer = new GenericContainer(
                            "confluentinc/cp-schema-registry:latest")
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
                                            + " --property value.schema=\"$(< order_detail.avsc)\" < file.txt && "
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
                        producerContainer.start();
                        assertTrue(sent.await(60, TimeUnit.SECONDS));
                    }

                    assertTrue(received.await(60, TimeUnit.SECONDS));
                }
            }
        }
    }

}
