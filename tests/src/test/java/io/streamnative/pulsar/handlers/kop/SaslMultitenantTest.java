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

import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Testing multi tenant features on KoP.
 */
@Test
@Slf4j
public class SaslMultitenantTest extends KopProtocolHandlerTestBase {

    protected static final String USER1 = "user1";
    protected static final String USER2 = "user2";
    protected static final String TENANT1 = "tenant1";
    protected static final String TENANT2 = "tenant2";
    protected static final String ADMIN_USER = "admin_user";
    private static final String PROXY_USER = "proxy_user";
    private static final String NAMESPACE = "kafka";
    private static final String NAMESPACE_NON_DEFAULT = "nondefault";
    private String adminToken;
    private String user1Token;
    private String user2Token;
    protected String proxyToken;

    public SaslMultitenantTest() {
        super("pulsar");
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));
        ServiceConfiguration authConf = new ServiceConfiguration();
        authConf.setProperties(properties);
        provider.initialize(authConf);

        user1Token = AuthTokenUtils.createToken(secretKey, USER1, Optional.empty());
        user2Token = AuthTokenUtils.createToken(secretKey, USER2, Optional.empty());
        adminToken = AuthTokenUtils.createToken(secretKey, ADMIN_USER, Optional.empty());
        proxyToken = AuthTokenUtils.createToken(secretKey, PROXY_USER, Optional.empty());

        super.resetConfig();
        authConf.setAllowAutoTopicCreation(true);
        conf.setKafkaNamespace(NAMESPACE);
        conf.setKafkaTransactionCoordinatorEnabled(true);
        conf.setProxyRoles(Sets.newHashSet(PROXY_USER));
        conf.setKopAllowedNamespaces(Collections
                .singleton(KafkaServiceConfiguration.TENANT_PLACEHOLDER + "/" + NAMESPACE));
        ((KafkaServiceConfiguration) conf).setSaslAllowedMechanisms(Sets.newHashSet("PLAIN"));
        ((KafkaServiceConfiguration) conf).setKafkaMetadataTenant("DONT-USE-ME");
        ((KafkaServiceConfiguration) conf).setKafkaMetadataNamespace("__kafka");

        conf.setClusterName(super.configClusterName);
        conf.setAuthorizationEnabled(true);
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationAllowWildcardsMatching(true);
        conf.setSuperUserRoles(Sets.newHashSet(ADMIN_USER, PROXY_USER));
        conf.setAuthenticationProviders(
            Sets.newHashSet("org.apache.pulsar.broker.authentication."
                + "AuthenticationProviderToken"));
        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters("token:" + adminToken);
        conf.setProperties(properties);

        super.internalSetup();

        admin.tenants().createTenant(TENANT1,
            TenantInfo.builder()
                    .adminRoles(Collections.singleton(USER1))
                    .allowedClusters(Collections.singleton(configClusterName))
                    .build());
        admin.namespaces().createNamespace(TENANT1 + "/" + NAMESPACE);
        admin.namespaces().createNamespace(TENANT1 + "/" + NAMESPACE_NON_DEFAULT);

        admin.tenants().createTenant(TENANT2,
                TenantInfo.builder()
                        .adminRoles(Collections.singleton(USER2))
                        .allowedClusters(Collections.singleton(configClusterName))
                        .build());
        admin.namespaces().createNamespace(TENANT2 + "/" + NAMESPACE);
        admin.namespaces().createNamespace(TENANT2 + "/" + NAMESPACE_NON_DEFAULT);
    }

    @Override
    protected void createAdmin() throws Exception {
        super.admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
            .authentication(this.conf.getBrokerClientAuthenticationPlugin(),
                this.conf.getBrokerClientAuthenticationParameters()).build());
    }

    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    void simpleProduceAndConsumeUnqualifiedTopicName() throws Exception {
        String topic = "test";
        test(topic, topic, true, topic);
    }

    @Test
    void simpleProduceAndConsumeQualifiedTopicName() throws Exception {
        String topic1 = TENANT1 + "/" + NAMESPACE + "/testFQ";
        String topic2 = TENANT2 + "/" + NAMESPACE + "/testFQ";
        test(topic1, topic2, true, "testFQ");
    }

    @Test
    void simpleProduceAndConsumePulsarQualifiedTopicName() throws Exception {
        String topic1 = "persistent://" + TENANT1 + "/" + NAMESPACE + "/testPFQ";
        String topic2 = "persistent://" + TENANT2 + "/" + NAMESPACE + "/testPFQ";
        test(topic1, topic2, true, "testPFQ");
    }

    @Test
    void simpleProduceAndConsumeQualifiedTopicNameNonDefaultNamespace() throws Exception {
        String topic1 = TENANT1 + "/" + NAMESPACE_NON_DEFAULT + "/testFQND";
        String topic2 = TENANT2 + "/" + NAMESPACE_NON_DEFAULT + "/testFQND";
        test(topic1, topic2, false, "testFQND");
    }

    private void test(String topic1, String topic2, boolean visibleInListTopics,
                      String nameForListTopics) throws Exception {

        @Cleanup
        KProducer kProducer1 = new KProducer(topic1, false, "localhost", getClientPort(),
                TENANT1, "token:" + user1Token);
        kProducer1.getProducer().send(new ProducerRecord<>(topic1, 0, "test to user1"));

        @Cleanup
        KProducer kProducer2 = new KProducer(topic2, false, "localhost", getClientPort(),
                TENANT2, "token:" + user2Token);
        kProducer2.getProducer().send(new ProducerRecord<>(topic2, 0, "test to user2"));

        @Cleanup
        KConsumer kConsumer1 = new KConsumer(topic1, "localhost", getClientPort(), false,
                TENANT1, "token:" + user1Token, "DemoKafkaOnPulsarConsumer");
        kConsumer1.getConsumer().subscribe(Collections.singleton(topic1));

        // second consumer, same group name, it is for a different tenant, so
        // the group is independent of the group of consumer1
        @Cleanup
        KConsumer kConsumer2 = new KConsumer(topic2, "localhost", getClientPort(), false,
                TENANT2, "token:" + user2Token, "DemoKafkaOnPulsarConsumer");
        kConsumer2.getConsumer().subscribe(Collections.singleton(topic2));

        ConsumerRecords<Integer, String> records1 = kConsumer1.getConsumer().poll(Duration.ofSeconds(10));
        assertEquals(1, records1.count());
        records1.forEach(r -> {
            assertEquals("test to user1", r.value());
        });

        // verify that we are not mixing data
        ConsumerRecords<Integer, String> records2 = kConsumer2.getConsumer().poll(Duration.ofSeconds(10));
        assertEquals(1, records2.count());
        records2.forEach(r -> {
            assertEquals("test to user2", r.value());
        });

        // ensure that we can list the topic
        Map<String, List<PartitionInfo>> result1 = kConsumer1
                .getConsumer().listTopics(Duration.ofSeconds(1));
        assertEquals(visibleInListTopics, result1.containsKey(nameForListTopics));

        // ensure that we can list the topic
        Map<String, List<PartitionInfo>> result2 = kConsumer2
                .getConsumer().listTopics(Duration.ofSeconds(1));
        assertEquals(visibleInListTopics, result2.containsKey(nameForListTopics));
    }
}
