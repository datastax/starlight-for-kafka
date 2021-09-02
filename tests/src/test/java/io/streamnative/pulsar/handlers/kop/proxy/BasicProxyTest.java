package io.streamnative.pulsar.handlers.kop.proxy;

import io.streamnative.pulsar.handlers.kop.BasicEndToEndTestBase;
import lombok.Cleanup;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;


public class BasicProxyTest extends BasicEndToEndTestBase {
    public BasicProxyTest() {
        super("pulsar");
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.setup();

        // pre-create system namespace, as it is needed by the proxy to discover Coordinators
        pulsar.getAdminClient().namespaces().createNamespace("public/__kafka");

        startProxy();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        stopProxy();
        super.cleanup();
    }

    @Test(timeOut = 60000)
    public void testProxy() throws Exception {
        final String topic = "test-proxy-works";

        // please note that in this case the Proxy will auto create the topic, as it does not exist
        @Cleanup
        final KafkaProducer<String, String> kafkaProducer = newKafkaProducer(bootstrapServersUsingProxy());
        sendSingleMessages(kafkaProducer, topic, Arrays.asList(null, ""));
        sendBatchedMessages(kafkaProducer, topic, Arrays.asList("test", null, ""));

        final List<String> expectValues = Arrays.asList(null, "", "test", null, "");

        @Cleanup
        final Consumer<byte[]> pulsarConsumer = newPulsarConsumer(topic);
        List<String> pulsarReceives = receiveMessages(pulsarConsumer, expectValues.size());
        assertEquals(pulsarReceives, expectValues);

        @Cleanup
        final KafkaConsumer<String, String> kafkaConsumer = newKafkaConsumer(topic, null, bootstrapServersUsingProxy());
        List<String> kafkaReceives = receiveMessages(kafkaConsumer, expectValues.size());
        assertEquals(kafkaReceives, expectValues);

    }
}
