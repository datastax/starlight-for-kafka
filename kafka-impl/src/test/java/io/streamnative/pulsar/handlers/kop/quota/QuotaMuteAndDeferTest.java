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
package io.streamnative.pulsar.handlers.kop.quota;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import io.streamnative.pulsar.handlers.kop.AdminManager;
import io.streamnative.pulsar.handlers.kop.EndPoint;
import io.streamnative.pulsar.handlers.kop.KafkaRequestHandler;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.KafkaTopicLookupService;
import io.streamnative.pulsar.handlers.kop.KafkaTopicManagerSharedState;
import io.streamnative.pulsar.handlers.kop.KopBrokerLookupManager;
import io.streamnative.pulsar.handlers.kop.LookupClient;
import io.streamnative.pulsar.handlers.kop.RequestStats;
import io.streamnative.pulsar.handlers.kop.TenantContextManager;
import io.streamnative.pulsar.handlers.kop.format.SchemaManager;
import io.streamnative.pulsar.handlers.kop.storage.ReplicaManager;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationPurgatory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.KopResponseUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;

public class QuotaMuteAndDeferTest {

    @Test
    public void testMuteDeferAndDrain() throws Exception {
        KafkaServiceConfiguration config = new KafkaServiceConfiguration();
        config.setKopClientQuotaEnabled(true);
        config.setKopClientQuotaMaxDeferredFrames(10);
        config.setKopClientQuotaMaxDeferredBytes(1024 * 1024L);
        config.setKopEnableGroupLevelConsumerMetrics(false);

        PulsarService pulsarService = mock(PulsarService.class);
        when(pulsarService.getConfiguration()).thenReturn(new ServiceConfiguration());
        assertNotNull(pulsarService.getConfiguration());
        PulsarResources pulsarResources = mock(PulsarResources.class);
        when(pulsarResources.getTopicResources()).thenReturn(mock(TopicResources.class));
        when(pulsarService.getPulsarResources()).thenReturn(pulsarResources);
        when(pulsarService.getNamespaceService()).thenReturn(mock(NamespaceService.class));
        BrokerService brokerService = mock(BrokerService.class);
        when(pulsarService.getBrokerService()).thenReturn(brokerService);
        when(brokerService.isAuthenticationEnabled()).thenReturn(false);
        when(brokerService.isAuthorizationEnabled()).thenReturn(false);
        when(brokerService.executor()).thenReturn(null);
        KopBrokerLookupManager kopBrokerLookupManager = mock(KopBrokerLookupManager.class);
        KafkaTopicManagerSharedState sharedState =
                new KafkaTopicManagerSharedState(brokerService, kopBrokerLookupManager);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        when(pulsarService.getExecutor()).thenReturn(executor);
        when(pulsarService.getAdminClient()).thenReturn(mock(PulsarAdmin.class));
        when(pulsarService.getLocalMetadataStore()).thenReturn(mock(MetadataStoreExtended.class));

        KafkaRequestHandler handler = new KafkaRequestHandler(
                pulsarService,
                config,
                mock(TenantContextManager.class),
                mock(ReplicaManager.class),
                kopBrokerLookupManager,
                mock(AdminManager.class),
                mock(DelayedOperationPurgatory.class),
                mock(DelayedOperationPurgatory.class),
                false,
                new EndPoint("PLAINTEXT://localhost:9092", Collections.emptyMap()),
                false,
                RequestStats.NULL_INSTANCE,
                null,
                sharedState,
                (Function<String, SchemaManager>) __ -> mock(SchemaManager.class),
                mock(KafkaTopicLookupService.class),
                mock(LookupClient.class),
                mock(ClientQuotaServiceManager.class));

        EmbeddedChannel channel = new EmbeddedChannel(handler);
        try {
            channel.pipeline().fireChannelActive();

            setPrivateBoolean(handler, "autoReadDisabledQuota", true);

            RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, "c1", 1);
            ApiVersionsRequest req = new ApiVersionsRequest.Builder().build();
            ByteBuf requestBuf = KopResponseUtils.serializeRequest(header, req);

            // Defer while muted: no outbound response should be produced.
            channel.eventLoop().execute(() -> channel.writeInbound(requestBuf));
            channel.runPendingTasks();

            assertNull(channel.readOutbound());
            assertTrue(requestBuf.refCnt() > 0, "Deferred frame should still be retained");

            // Unmute and drain deferred frames.
            channel.eventLoop().execute(() -> {
                try {
                    setPrivateBoolean(handler, "autoReadDisabledQuota", false);
                    invokePrivateNoArgs(handler, "drainDeferredQuotaFrames");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            channel.runPendingTasks();
            channel.runScheduledPendingTasks();

            ByteBuf responseBuf = channel.readOutbound();
            assertNotNull(responseBuf);
            ReferenceCountUtil.safeRelease(responseBuf);
            assertTrue(requestBuf.refCnt() == 0, "Deferred frame should be released after being drained");
        } finally {
            channel.finishAndReleaseAll();
            sharedState.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testDeferredOverflowClosesAndReleases() throws Exception {
        KafkaServiceConfiguration config = new KafkaServiceConfiguration();
        config.setKopClientQuotaEnabled(true);
        config.setKopClientQuotaMaxDeferredFrames(1);
        config.setKopClientQuotaMaxDeferredBytes(1024 * 1024L);
        config.setKopEnableGroupLevelConsumerMetrics(false);

        PulsarService pulsarService = mock(PulsarService.class);
        when(pulsarService.getConfiguration()).thenReturn(new ServiceConfiguration());
        assertNotNull(pulsarService.getConfiguration());
        PulsarResources pulsarResources = mock(PulsarResources.class);
        when(pulsarResources.getTopicResources()).thenReturn(mock(TopicResources.class));
        when(pulsarService.getPulsarResources()).thenReturn(pulsarResources);
        when(pulsarService.getNamespaceService()).thenReturn(mock(NamespaceService.class));
        BrokerService brokerService = mock(BrokerService.class);
        when(pulsarService.getBrokerService()).thenReturn(brokerService);
        when(brokerService.isAuthenticationEnabled()).thenReturn(false);
        when(brokerService.isAuthorizationEnabled()).thenReturn(false);
        when(brokerService.executor()).thenReturn(null);
        KopBrokerLookupManager kopBrokerLookupManager = mock(KopBrokerLookupManager.class);
        KafkaTopicManagerSharedState sharedState =
                new KafkaTopicManagerSharedState(brokerService, kopBrokerLookupManager);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        when(pulsarService.getExecutor()).thenReturn(executor);
        when(pulsarService.getAdminClient()).thenReturn(mock(PulsarAdmin.class));
        when(pulsarService.getLocalMetadataStore()).thenReturn(mock(MetadataStoreExtended.class));

        KafkaRequestHandler handler = new KafkaRequestHandler(
                pulsarService,
                config,
                mock(TenantContextManager.class),
                mock(ReplicaManager.class),
                kopBrokerLookupManager,
                mock(AdminManager.class),
                mock(DelayedOperationPurgatory.class),
                mock(DelayedOperationPurgatory.class),
                false,
                new EndPoint("PLAINTEXT://localhost:9092", Collections.emptyMap()),
                false,
                RequestStats.NULL_INSTANCE,
                null,
                sharedState,
                (Function<String, SchemaManager>) __ -> mock(SchemaManager.class),
                mock(KafkaTopicLookupService.class),
                mock(LookupClient.class),
                mock(ClientQuotaServiceManager.class));

        EmbeddedChannel channel = new EmbeddedChannel(handler);
        try {
            channel.pipeline().fireChannelActive();

            setPrivateBoolean(handler, "autoReadDisabledQuota", true);

            RequestHeader header1 = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, "c1", 1);
            RequestHeader header2 = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, "c1", 2);
            ApiVersionsRequest req = new ApiVersionsRequest.Builder().build();
            ByteBuf buf1 = KopResponseUtils.serializeRequest(header1, req);
            ByteBuf buf2 = KopResponseUtils.serializeRequest(header2, req);

            channel.eventLoop().execute(() -> {
                channel.writeInbound(buf1);
                channel.writeInbound(buf2);
            });
            channel.runPendingTasks();
            channel.runScheduledPendingTasks();

            assertTrue(buf1.refCnt() == 0, "Deferred frame should be released after overflow close");
            assertTrue(buf2.refCnt() == 0, "Deferred frame should be released after overflow close");
            assertTrue(!channel.isActive(), "Channel should be closed due to deferred overflow");
        } finally {
            channel.finishAndReleaseAll();
            sharedState.close();
            executor.shutdownNow();
        }
    }

    private static void setPrivateBoolean(Object target, String fieldName, boolean value) throws Exception {
        Field f = target.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        f.setBoolean(target, value);
    }

    private static void invokePrivateNoArgs(Object target, String methodName) throws Exception {
        Method m = target.getClass().getDeclaredMethod(methodName);
        m.setAccessible(true);
        m.invoke(target);
    }
}
