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

import static io.streamnative.pulsar.handlers.kop.KafkaProtocolHandler.TLS_HANDLER;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.SERVER_SCOPE;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import io.streamnative.pulsar.handlers.kop.format.SchemaManager;
import io.streamnative.pulsar.handlers.kop.stats.PrometheusMetricsProvider;
import io.streamnative.pulsar.handlers.kop.stats.StatsLogger;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperation;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationPurgatory;
import io.streamnative.pulsar.handlers.kop.utils.ssl.SSLUtils;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.Getter;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.broker.PulsarService;

/**
 * A channel initializer that initialize channels for kafka protocol.
 */
public class KafkaChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final int MAX_FRAME_LENGTH = 100 * 1024 * 1024; // 100MB

    @Getter
    private final PulsarService pulsarService;
    @Getter
    private final KafkaServiceConfiguration kafkaConfig;
    @Getter
    private final TenantContextManager tenantContextManager;
    @Getter
    private final KopBrokerLookupManager kopBrokerLookupManager;
    @Getter
    private final KafkaTopicManagerSharedState kafkaTopicManagerSharedState;
    private final Function<String, SchemaManager> schemaManagerForTenant;

    private final AdminManager adminManager;
    private DelayedOperationPurgatory<DelayedOperation> producePurgatory;
    private DelayedOperationPurgatory<DelayedOperation> fetchPurgatory;
    @Getter
    private final boolean enableTls;
    @Getter
    private final EndPoint advertisedEndPoint;
    private final boolean skipMessagesWithoutIndex;
    private SSLUtils.ServerSideTLSSupport tlsSupport;
    @Getter
    private final RequestStats requestStats;
    private final OrderedScheduler sendResponseScheduler;

    private final LengthFieldPrepender lengthFieldPrepender;

    public KafkaChannelInitializer(PulsarService pulsarService,
                                   KafkaServiceConfiguration kafkaConfig,
                                   TenantContextManager tenantContextManager,
                                   KopBrokerLookupManager kopBrokerLookupManager,
                                   AdminManager adminManager,
                                   DelayedOperationPurgatory<DelayedOperation> producePurgatory,
                                   DelayedOperationPurgatory<DelayedOperation> fetchPurgatory,
                                   boolean enableTLS,
                                   EndPoint advertisedEndPoint,
                                   boolean skipMessagesWithoutIndex,
                                   RequestStats requestStats,
                                   OrderedScheduler sendResponseScheduler,
                                   KafkaTopicManagerSharedState kafkaTopicManagerSharedState,
                                   Function<String, SchemaManager> schemaManagerForTenant) {
        super();
        this.schemaManagerForTenant = schemaManagerForTenant;
        this.pulsarService = pulsarService;
        this.kafkaConfig = kafkaConfig;
        this.tenantContextManager = tenantContextManager;
        this.kopBrokerLookupManager = kopBrokerLookupManager;
        this.adminManager = adminManager;
        this.producePurgatory = producePurgatory;
        this.fetchPurgatory = fetchPurgatory;
        this.enableTls = enableTLS;
        this.advertisedEndPoint = advertisedEndPoint;
        this.skipMessagesWithoutIndex = skipMessagesWithoutIndex;
        this.requestStats = requestStats;
        if (enableTls) {
            tlsSupport = new SSLUtils.ServerSideTLSSupport(kafkaConfig);
        } else {
            tlsSupport = null;
        }
        this.sendResponseScheduler = sendResponseScheduler;
        this.kafkaTopicManagerSharedState = kafkaTopicManagerSharedState;
        this.lengthFieldPrepender = new LengthFieldPrepender(4);
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast("idleStateHandler",
                new IdleStateHandler(
                        kafkaConfig.getConnectionMaxIdleMs(),
                        kafkaConfig.getConnectionMaxIdleMs(),
                        0,
                        TimeUnit.MILLISECONDS));
        if (tlsSupport != null) {
            tlsSupport.addTlsHandler(ch);
        }
        ch.pipeline().addLast(lengthFieldPrepender);
        ch.pipeline().addLast("frameDecoder",
            new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
        ch.pipeline().addLast("handler", newCnx());
    }

    @VisibleForTesting
    public KafkaRequestHandler newCnx() throws Exception {
        return new KafkaRequestHandler(pulsarService, kafkaConfig,
                tenantContextManager, kopBrokerLookupManager, adminManager,
                producePurgatory, fetchPurgatory,
                enableTls, advertisedEndPoint, skipMessagesWithoutIndex, requestStats, sendResponseScheduler,
                kafkaTopicManagerSharedState, schemaManagerForTenant);
    }

    @VisibleForTesting
    public KafkaRequestHandler newCnx(final TenantContextManager tenantContextManager) throws Exception {
        PrometheusMetricsProvider statsProvider = new PrometheusMetricsProvider();
        StatsLogger rootStatsLogger = statsProvider.getStatsLogger("");
        return new KafkaRequestHandler(pulsarService, kafkaConfig,
                tenantContextManager, kopBrokerLookupManager, adminManager,
                producePurgatory, fetchPurgatory,
                enableTls, advertisedEndPoint, skipMessagesWithoutIndex,
                new RequestStats(rootStatsLogger.scope(SERVER_SCOPE)),
                sendResponseScheduler,
                kafkaTopicManagerSharedState, schemaManagerForTenant);
    }

}
