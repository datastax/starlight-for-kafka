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
import org.testng.annotations.Test;

@Slf4j
public class DockerTest {

    @Test
    public void test() throws Exception {
        test("pulsar:9092", false);
    }

    @Test
    public void testProxy() throws Exception {
        test("pulsarproxy:9092", true);
    }

    private void test(String kafkaAddress, boolean proxy) throws Exception {
        // create a docker network
        try (Network network = Network.newNetwork();) {
            // start Pulsar and wait for it to be ready to accept requests
            try (PulsarContainer pulsarContainer = new PulsarContainer(network, proxy);) {
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
                                    "echo This-is-my-message > file.txt && " +
                                            "kafka-console-producer --bootstrap-server " + kafkaAddress
                                            + "  --topic test < file.txt && " +
                                            "echo FINISHEDPRODUCER")
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
