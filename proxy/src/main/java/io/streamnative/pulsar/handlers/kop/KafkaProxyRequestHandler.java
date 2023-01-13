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

import static com.google.common.base.Preconditions.checkArgument;
import static io.streamnative.pulsar.handlers.kop.KafkaRequestHandler.newNode;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataManager;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionConfig;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import io.streamnative.pulsar.handlers.kop.exceptions.KoPTopicException;
import io.streamnative.pulsar.handlers.kop.security.SaslAuthenticator;
import io.streamnative.pulsar.handlers.kop.security.Session;
import io.streamnative.pulsar.handlers.kop.security.auth.Authorizer;
import io.streamnative.pulsar.handlers.kop.security.auth.PulsarMetadataAccessor;
import io.streamnative.pulsar.handlers.kop.security.auth.Resource;
import io.streamnative.pulsar.handlers.kop.security.auth.ResourceType;
import io.streamnative.pulsar.handlers.kop.security.auth.SimpleAclAuthorizer;
import io.streamnative.pulsar.handlers.kop.utils.KafkaResponseUtils;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.MetadataUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.DeleteGroupsRequest;
import org.apache.kafka.common.requests.DeleteRecordsRequest;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.KopResponseUtils;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListOffsetRequestV0;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.AuthenticationUtil;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * This class contains all the request handling methods.
 */
@Slf4j
@Getter
public class KafkaProxyRequestHandler extends KafkaCommandDecoder {

    final String id;
    private final KafkaProtocolProxyMain.PulsarAdminProvider admin;
    private final SaslAuthenticator authenticator;
    private final Authorizer authorizer;
    // this is for Proxy -> Broker authentication
    private final Authentication authenticationToken;
    private final boolean tlsEnabled;
    private final EndPoint advertisedEndPoint;
    private final String advertisedListeners;
    private final ConcurrentHashMap<String, Node> topicsLeaders;
    private final Function<String, String> brokerAddressMapper;
    private final EventLoopGroup workerGroup;
    private final ConcurrentHashMap<String, ConnectionToBroker> connectionsToBrokers = new ConcurrentHashMap<>();
    private AtomicInteger dummyCorrelationIdGenerator = new AtomicInteger(-1);
    private volatile boolean coordinatorNamespaceExists = false;

    public KafkaProxyRequestHandler(String id, KafkaProtocolProxyMain.PulsarAdminProvider pulsarAdmin,
                                    AuthenticationService authenticationService,
                                    AuthorizationService authorizationService,
                                    KafkaServiceConfiguration kafkaConfig,
                                    boolean tlsEnabled,
                                    EndPoint advertisedEndPoint,
                                    Function<String, String> brokerAddressMapper,
                                    EventLoopGroup workerGroup,
                                    RequestStats requestStats,
                                    ConcurrentHashMap<String, Node> topicsLeaders) throws Exception {
        super(requestStats, kafkaConfig, null);
        this.topicsLeaders = topicsLeaders;
        this.workerGroup = workerGroup;
        this.brokerAddressMapper = brokerAddressMapper;
        this.id = id;
        String auth = kafkaConfig.getBrokerClientAuthenticationPlugin();
        String authParams = kafkaConfig.getBrokerClientAuthenticationParameters();
        this.authenticationToken = AuthenticationUtil.create(auth, authParams);

        this.admin = pulsarAdmin;
        final boolean authenticationEnabled = kafkaConfig.isAuthenticationEnabled();
        this.authenticator = authenticationEnabled
                ? new SaslAuthenticator(null, authenticationService,
                kafkaConfig.getSaslAllowedMechanisms(), kafkaConfig)
                : null;
        final boolean authorizationEnabled = kafkaConfig.isAuthorizationEnabled();
        this.authorizer = authorizationEnabled && authenticationEnabled
                ? new SimpleAclAuthorizer(new PulsarMetadataAccessor.PulsarAdminMetadataAccessor(() -> {
            try {
                return CompletableFuture
                        .completedFuture(admin.getAdminForPrincipal(kafkaConfig.getKafkaProxySuperUserRole()));
            } catch (Exception err) {
                return FutureUtil.failedFuture(err);
            }
        }, kafkaConfig, authorizationService))
                : null;
        this.tlsEnabled = tlsEnabled;
        this.advertisedEndPoint = advertisedEndPoint;
        this.advertisedListeners = kafkaConfig.getKafkaAdvertisedListeners();
    }

    private static String extractTenantFromTenantSpec(String tenantSpec) {
        if (tenantSpec != null && !tenantSpec.isEmpty()) {
            String tenant = tenantSpec;
            // username can be "tenant" or "tenant/namespace"
            if (tenantSpec.contains("/")) {
                tenant = tenantSpec.substring(0, tenantSpec.indexOf('/'));
            }
            log.debug("using {} as tenant", tenant);
            return tenant;
        } else {
            return tenantSpec;
        }
    }

    static KafkaResponseUtils.BrokerLookupResult newPartitionMetadata(TopicName topicName, Node node) {
        int pulsarPartitionIndex = topicName.getPartitionIndex();
        int kafkaPartitionIndex = pulsarPartitionIndex == -1 ? 0 : pulsarPartitionIndex;

        if (log.isDebugEnabled()) {
            log.debug("Return PartitionMetadata node: {}, topicName: {}", node, topicName);
        }
        TopicPartition topicPartition = new TopicPartition(topicName.toString(), kafkaPartitionIndex);
        return KafkaResponseUtils.newMetadataPartition(topicPartition, node);
    }

    @Override
    protected void channelPrepare(ChannelHandlerContext ctx,
                                  ByteBuf requestBuf,
                                  BiConsumer<Long, Throwable> registerRequestParseLatency,
                                  BiConsumer<ApiKeys, Long> registerRequestLatency)
            throws AuthenticationException {
        if (authenticator != null) {
            authenticator.authenticate(ctx, requestBuf, registerRequestParseLatency, registerRequestLatency,
                    this::validateTenantAccessForSession);
            if (authenticator.complete() && kafkaConfig.isKafkaEnableMultiTenantMetadata()) {
                setRequestStats(requestStats.forTenant(getCurrentTenant()));
            }
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        log.debug("Client connected: {}", ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        log.debug("Client disconnected {}", ctx.channel());
        close();
    }

    @Override
    protected void maybeDelayCloseOnAuthenticationFailure() {
        if (this.kafkaConfig.getFailedAuthenticationDelayMs() > 0) {
            this.ctx.executor().schedule(
                    this::handleCloseOnAuthenticationFailure,
                    this.kafkaConfig.getFailedAuthenticationDelayMs(),
                    TimeUnit.MILLISECONDS);
        } else {
            handleCloseOnAuthenticationFailure();
        }
    }

    private void handleCloseOnAuthenticationFailure() {
        try {
            this.completeCloseOnAuthenticationFailure();
        } finally {
            this.close();
        }
    }

    @Override
    protected void completeCloseOnAuthenticationFailure() {
        if (isActive.get() && authenticator != null) {
            authenticator.sendAuthenticationFailureResponse(__ -> {});
        }
    }

    @Override
    protected void close() {
        if (isActive.getAndSet(false)) {
            super.close();
            connectionsToBrokers.values().forEach(c -> {
                c.close();
            });
        }
    }

    @Override
    protected boolean hasAuthenticated() {
        return authenticator == null || authenticator.complete();
    }

    @Override
    protected void handleApiVersionsRequest(KafkaHeaderAndRequest apiVersionRequest,
                                            CompletableFuture<AbstractResponse> resultFuture) {
        if (!ApiKeys.API_VERSIONS.isVersionSupported(apiVersionRequest.getHeader().apiVersion())) {
            // Notify Client that API_VERSION is UNSUPPORTED.
            AbstractResponse apiResponse = overloadDefaultApiVersionsResponse(true);
            resultFuture.complete(apiResponse);
        } else {
            AbstractResponse apiResponse = overloadDefaultApiVersionsResponse(false);
            resultFuture.complete(apiResponse);
        }
    }

    protected ApiVersionsResponse overloadDefaultApiVersionsResponse(boolean unsupportedApiVersion) {
        if (unsupportedApiVersion){
            return KafkaResponseUtils.newApiVersions(Errors.UNSUPPORTED_VERSION);
        } else {
            List<ApiVersion> versionList = new ArrayList<>();
            for (ApiKeys apiKey : ApiKeys.values()) {
                if (apiKey.minRequiredInterBrokerMagic <= RecordBatch.CURRENT_MAGIC_VALUE) {
                    switch (apiKey) {
                        case LIST_OFFSETS:
                            // V0 is needed for librdkafka
                            versionList.add(new ApiVersion((short) 2, (short) 0, apiKey.latestVersion()));
                            break;
                        default:
                            versionList.add(new ApiVersion(apiKey));
                    }
                }
            }
            return KafkaResponseUtils.newApiVersions(versionList);
        }
    }

    protected void handleError(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                               CompletableFuture<AbstractResponse> resultFuture) {
        String err = String.format("Kafka API (%s) Not supported by kop server.",
                kafkaHeaderAndRequest.getHeader().apiKey());
        log.error(err);

        AbstractResponse apiResponse = kafkaHeaderAndRequest.getRequest()
                .getErrorResponse(new UnsupportedOperationException(err));
        resultFuture.complete(apiResponse);
    }

    protected void handleInactive(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                  CompletableFuture<AbstractResponse> resultFuture) {
        AbstractRequest request = kafkaHeaderAndRequest.getRequest();
        AbstractResponse apiResponse = request.getErrorResponse(new LeaderNotAvailableException("Channel is closing!"));

        log.error("Kafka API {} is send to a closing channel", kafkaHeaderAndRequest.getHeader().apiKey());

        resultFuture.complete(apiResponse);
    }

    protected void handleTopicMetadataRequest(KafkaHeaderAndRequest metadataHar,
                                              CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(metadataHar.getRequest() instanceof MetadataRequest);

        log.debug("handleTopicMetadataRequest {}", metadataHar);

        // just pass the request to any broker

        CompletableFuture<AbstractResponse> responseInterceptor = new CompletableFuture<>();

        handleRequestWithCoordinator(metadataHar, responseInterceptor, FindCoordinatorRequest.CoordinatorType.GROUP,
                MetadataRequest.class,
                MetadataRequestData.class,
                (metadataRequest) -> "system",
                null);

        String namespacePrefix = currentNamespacePrefix();
        responseInterceptor.whenComplete((metadataResponse, error) -> {
            if (error != null) {
                resultFuture.completeExceptionally(error);
            } else {
                MetadataResponse responseFromBroker = (MetadataResponse) metadataResponse;
                Node selfNode = newSelfNode();

                MetadataResponseData response = new MetadataResponseData()
                        .setThrottleTimeMs(responseFromBroker.throttleTimeMs())
                        .setClusterId(responseFromBroker.clusterId())
                                .setControllerId(selfNode.id());
                response.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                        .setHost(selfNode.host())
                        .setPort(selfNode.port())
                        .setNodeId(selfNode.id()));

                        responseFromBroker.topicMetadata()
                                .stream()
                                .forEach((MetadataResponse.TopicMetadata md) -> {
                                    MetadataResponseData.MetadataResponseTopic metadataResponseTopic =
                                            new MetadataResponseData.MetadataResponseTopic()
                                            .setName(md.topic())
                                            .setErrorCode(md.error().code())
                                            .setIsInternal(md.isInternal());
                                    response.topics().add(metadataResponseTopic);
                                            md.partitionMetadata()
                                                    .forEach((MetadataResponse.PartitionMetadata pd) -> {
                                                        // please note that usually the Kafka client
                                                        // opens two different connections
                                                        // for metadata and for data
                                                        // so caching this value here
                                                        // won't help to serve Produce or Fetch requests
                                                        String fullTopicName = KopTopic.toString(md.topic(),
                                                                pd.partition(), namespacePrefix);
                                                        pd.leaderId.ifPresent(leaderId -> {
                                                            Node node = responseFromBroker.brokersById().get(leaderId);
                                                            topicsLeaders.put(fullTopicName, node);
                                                            log.info("Leader for {} is {}", fullTopicName, pd.leaderId);
                                                        });

                                                        metadataResponseTopic.partitions().add(
                                                            new MetadataResponseData.MetadataResponsePartition()
                                                            .setPartitionIndex(pd.partition())
                                                            .setErrorCode(md.error().code())
                                                            .setLeaderId(selfNode.id())
                                                            .setIsrNodes(Collections.singletonList(selfNode.id()))
                                                            .setOfflineReplicas(Collections.emptyList())
                                                            .setReplicaNodes(Collections.singletonList(selfNode.id())));
                                                    });
                                });
                resultFuture.complete(new MetadataResponse(response,
                        metadataHar.getRequest().version()));
            }
        });
    }

    protected void handleProduceRequest(KafkaHeaderAndRequest produceHar,
                                        CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(produceHar.getRequest() instanceof ProduceRequest);
        ProduceRequest produceRequest = (ProduceRequest) produceHar.getRequest();
        ProduceRequestData data = produceRequest.data();
        Map<TopicPartition, ProduceRequestData.PartitionProduceData> partitionRecords = new HashMap<>();
        data.topicData().forEach(topic -> {
            topic.partitionData().forEach((ProduceRequestData.PartitionProduceData partitionProduceData) -> {
                partitionRecords.put(new TopicPartition(topic.name(), partitionProduceData.index()),
                        partitionProduceData);
            });
        });
        final int numPartitions = partitionRecords.size();
        if (numPartitions == 0) {
            resultFuture.complete(new ProduceResponse(new HashMap<>()));
            return;
        }

        final Map<TopicPartition, PartitionResponse> responseMap = new ConcurrentHashMap<>();
        // delay produce
        final AtomicInteger topicPartitionNum = new AtomicInteger(partitionRecords.size());

        String namespacePrefix = currentNamespacePrefix();
        final String metadataNamespace = kafkaConfig.getKafkaMetadataNamespace();
        // validate system topics
        for (TopicPartition topicPartition : partitionRecords.keySet()) {
            final String fullPartitionName = KopTopic.toString(topicPartition, namespacePrefix);
            // check KOP inner topic
            if (KopTopic.isInternalTopic(metadataNamespace, fullPartitionName)) {
                log.error("[{}] Request {}: not support produce message to inner topic. topic: {}",
                        ctx.channel(), produceHar.getHeader(), topicPartition);
                Map<TopicPartition, PartitionResponse> errorsMap =
                        partitionRecords
                                .keySet()
                                .stream()
                                .collect(Collectors.toMap(Function.identity(),
                                        p -> new PartitionResponse(Errors.INVALID_TOPIC_EXCEPTION)));
                resultFuture.complete(new ProduceResponse(errorsMap));
                return;
            }
        }

        Map<String, KafkaResponseUtils.BrokerLookupResult> brokers = new ConcurrentHashMap<>();
        List<CompletableFuture<?>> lookups = new ArrayList<>(topicPartitionNum.get());
        partitionRecords.forEach((topicPartition, records) -> {
            final String fullPartitionName = KopTopic.toString(topicPartition, namespacePrefix);
            lookups.add(findBroker(TopicName.get(fullPartitionName))
                    .thenAccept(p -> brokers.put(fullPartitionName, p)));
        });

        // this looks weird
        // we must block here, if we continue the execution of this Produce Request
        // in other thread we will break strict ordering of messages
        try {
            FutureUtil.waitForAll(lookups).get(kafkaConfig.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException | InterruptedException | TimeoutException err) {
            log.error("Cannot lookup brokers for a produce request {}", produceRequest, err);
            Map<TopicPartition, PartitionResponse> errorsMap =
                    partitionRecords
                            .keySet()
                            .stream()
                            .collect(Collectors.toMap(Function.identity(),
                                    p -> new PartitionResponse(Errors.REQUEST_TIMED_OUT)));
            resultFuture.complete(new ProduceResponse(errorsMap));
            return;
        }

        boolean multipleBrokers = false;
        // check if all the partitions are for the same broker
        KafkaResponseUtils.BrokerLookupResult first = null;
        for (KafkaResponseUtils.BrokerLookupResult md : brokers.values()) {
            if (first == null) {
                first = md;
            } else if (!Objects.equals(first.node, md.node)) {
                multipleBrokers = true;
                break;
            }
        }


        if (!multipleBrokers) {
            // all the partitions are owned by one single broker,
            // we can forward the whole request to the only broker
            final KafkaResponseUtils.BrokerLookupResult broker = first;
            log.debug("forward FULL produce id {} of {} parts to {}", produceHar.getHeader().correlationId(),
                    numPartitions, broker);
            grabConnectionToBroker(broker.node.host(), broker.node.port()).
                    forwardRequest(produceHar)
                    .thenAccept(response -> {
                        ProduceResponse resp = (ProduceResponse) response;
                        ProduceResponseData singleBrokerData = resp.data();
                        singleBrokerData.responses().forEach(topic -> {
                            topic.partitionResponses().forEach(partitionProduceResponse -> {
                                TopicPartition topicPartition = new TopicPartition(topic.name(),
                                        partitionProduceResponse.index());
                                invalidateLeaderIfNeeded(namespacePrefix, broker.node,
                                        topicPartition, partitionProduceResponse.errorCode());
                                if (log.isDebugEnabled()
                                        && partitionProduceResponse.errorCode() == Errors.NONE.code()) {
                                    log.debug("forward FULL produce id {} COMPLETE  of {} parts to {}",
                                            produceHar.getHeader().correlationId(), numPartitions, broker);
                                }
                            });
                        });
                        resultFuture.complete(response);
                    }).exceptionally(badError -> {
                        log.error("Full Produce failed", badError);
                        // REQUEST_TIMED_OUT triggers a new trials on the client
                        Map<TopicPartition, PartitionResponse> errorsMap =
                                partitionRecords
                                        .keySet()
                                        .stream()
                                        .collect(Collectors.toMap(Function.identity(),
                                                p -> new PartitionResponse(Errors.REQUEST_TIMED_OUT)));
                        resultFuture.complete(new ProduceResponse(errorsMap));
                        return null;
                    });
        } else {
            log.debug("Split produce of {} parts to {}", numPartitions, brokers);
            // we have to create multiple ProduceRequest
            // this is a prototype, let's create a ProduceRequest per each partition
            // we could group requests per broker

            Runnable complete = () -> {
                log.debug("complete produce {}", produceHar);
                topicPartitionNum.set(0);
                if (resultFuture.isDone()) {
                    // It may be triggered again in DelayedProduceAndFetch
                    return;
                }
                // add the topicPartition with timeout error if it's not existed in responseMap
                partitionRecords.keySet().forEach(topicPartition -> {
                    if (!responseMap.containsKey(topicPartition)) {
                        responseMap.put(topicPartition, new PartitionResponse(Errors.REQUEST_TIMED_OUT));
                    }
                });
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Request {}: Complete handle produce.", ctx.channel(), produceHar.toString());
                }
                resultFuture.complete(new ProduceResponse(responseMap));
            };
            BiConsumer<TopicPartition, PartitionResponse> addPartitionResponse = (topicPartition, response) -> {

                responseMap.put(topicPartition, response);
                // reset topicPartitionNum
                int restTopicPartitionNum = topicPartitionNum.decrementAndGet();
                log.debug("addPartitionResponse {} {} restTopicPartitionNum {}", topicPartition,
                        response, restTopicPartitionNum);
                if (restTopicPartitionNum < 0) {
                    return;
                }
                if (restTopicPartitionNum == 0) {
                    complete.run();
                }
            };

            // split the request per broker
            final Map<Node, ProduceRequestData> requestsPerBroker = new HashMap<>();
            partitionRecords.forEach((topicPartition, records) -> {
                final String fullPartitionName = KopTopic.toString(topicPartition, namespacePrefix);
                KafkaResponseUtils.BrokerLookupResult topicMetadata = brokers.get(fullPartitionName);
                Node kopBroker = topicMetadata.node;

                ProduceRequestData produceRequestPerBroker = requestsPerBroker.computeIfAbsent(kopBroker,
                        a -> new ProduceRequestData());
                ProduceRequestData.TopicProduceData topicProduceData = produceRequestPerBroker
                        .topicData()
                        .stream().filter(topic -> topic.name().equals(topicPartition.topic()))
                        .findFirst().orElse(null);
                if (topicProduceData == null) {
                    topicProduceData = new ProduceRequestData.TopicProduceData()
                            .setName(topicPartition.topic());
                    produceRequestPerBroker.topicData().add(topicProduceData);
                }
                topicProduceData.partitionData().add(new ProduceRequestData.PartitionProduceData()
                        .setIndex(records.index())
                        .setRecords(records.records()));
            });
            requestsPerBroker.forEach((kopBroker, requestForSinglePartition) -> {
                int dummyCorrelationId = getDummyCorrelationId();

                RequestHeader header = new RequestHeader(
                        produceHar.getHeader().apiKey(),
                        produceHar.getHeader().apiVersion(),
                        produceHar.getHeader().clientId(),
                        dummyCorrelationId
                );

                ProduceRequest produceReq = new ProduceRequest.Builder(
                        produceRequest.version(), produceRequest.version(), requestForSinglePartition)
                        .buildUnsafe(produceRequest.version());
                ByteBuf buffer = KopResponseUtils.serializeRequest(header, produceReq);

                KafkaHeaderAndRequest singlePartitionRequest = new KafkaHeaderAndRequest(
                        header,
                        produceReq,
                        buffer,
                        null
                );
                buffer.release();

                if (log.isDebugEnabled()) {
                    log.debug("forward produce for {} to {}",
                            requestForSinglePartition
                                    .topicData().stream().map(ProduceRequestData.TopicProduceData::name)
                                    .collect(Collectors.toList()),
                            kopBroker);
                }
                grabConnectionToBroker(kopBroker.host(), kopBroker.port())
                        .forwardRequest(singlePartitionRequest)
                        .thenAccept(response -> {
                            ProduceResponse resp = (ProduceResponse) response;
                            resp.responses().forEach((topicPartition, partitionResponse) -> {
                                if (partitionResponse.error == Errors.NONE) {
                                    log.debug("result produce for {} to {} {}", topicPartition,
                                            kopBroker, partitionResponse);
                                    addPartitionResponse.accept(topicPartition, partitionResponse);
                                } else {
                                    invalidateLeaderIfNeeded(namespacePrefix, kopBroker, topicPartition,
                                            partitionResponse.error);
                                    addPartitionResponse.accept(topicPartition, partitionResponse);
                                }
                            });
                        }).exceptionally(badError -> {
                            log.error("bad error during split produce for {}",
                                    requestForSinglePartition.topicData().stream()
                                            .map(ProduceRequestData.TopicProduceData::name)
                                            .collect(Collectors.toList()), badError);
                            requestForSinglePartition.topicData().forEach(topic -> {
                                topic.partitionData().forEach(partitionProduceData -> {
                                    addPartitionResponse.accept(
                                            new TopicPartition(topic.name(), partitionProduceData.index()),
                                            new PartitionResponse(Errors.REQUEST_TIMED_OUT));
                                });
                            });
                            return null;
                        }).whenComplete((ignore1, ignore2) -> {
                            singlePartitionRequest.close();
                        });
            });
        }
    }

    private void invalidateLeaderIfNeeded(String namespacePrefix, Node kopBroker, TopicPartition topicPartition,
                                          Errors error) {
        invalidateLeaderIfNeeded(namespacePrefix, kopBroker, topicPartition, error.code());
    }
    private void invalidateLeaderIfNeeded(String namespacePrefix, Node kopBroker, TopicPartition topicPartition,
                                          short error) {
        if (error == Errors.NOT_LEADER_OR_FOLLOWER.code()) {
            final String fullPartitionName = KopTopic.toString(topicPartition, namespacePrefix);
            log.info("Broker {} is no more the leader for {} - {} (topicsLeaders {})",
                    kopBroker,
                    topicPartition,
                    fullPartitionName,
                    topicsLeaders);
            topicsLeaders.remove(fullPartitionName);
        }
    }

    int getDummyCorrelationId() {
        return dummyCorrelationIdGenerator.decrementAndGet();
    }

    protected void handleFetchRequest(KafkaHeaderAndRequest fetch,
                                      CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(fetch.getRequest() instanceof FetchRequest);
        FetchRequest fetchRequest = (FetchRequest) fetch.getRequest();
        Map<TopicPartition, FetchRequest.PartitionData> fetchData = fetchRequest.fetchData();

        final int numPartitions = fetchData.size();
        if (numPartitions == 0) {
            resultFuture.complete(new FetchResponse(Errors.NONE, new LinkedHashMap<>(), 0,
                    fetchRequest.metadata().sessionId()));
            return;
        }

        String namespacePrefix = currentNamespacePrefix();
        Map<TopicPartition, FetchResponse.PartitionData<?>> responseMap = new ConcurrentHashMap<>();
        final AtomicInteger topicPartitionNum = new AtomicInteger(fetchData.size());
        final String metadataNamespace = kafkaConfig.getKafkaMetadataNamespace();

        // validate system topics
        for (TopicPartition topicPartition : fetchData.keySet()) {
            final String fullPartitionName = KopTopic.toString(topicPartition, namespacePrefix);
            // check KOP inner topic
            if (KopTopic.isInternalTopic(metadataNamespace, fullPartitionName)) {
                log.error("[{}] Request {}: not support fetch message to inner topic. topic: {}",
                        ctx.channel(), fetch.getHeader(), topicPartition);
                Map<TopicPartition, FetchResponse.PartitionData<?>> errorsMap =
                        fetchData
                                .keySet()
                                .stream()
                                .collect(Collectors.toMap(Function.identity(),
                                        p -> new FetchResponse.PartitionData(Errors.INVALID_TOPIC_EXCEPTION,
                                                0, 0, 0,
                                                null, MemoryRecords.EMPTY)));
                resultFuture.complete(new FetchResponse(Errors.INVALID_TOPIC_EXCEPTION,
                        new LinkedHashMap<>(errorsMap), 0, fetchRequest.metadata().sessionId()));
                return;
            }
        }

        Map<String, KafkaResponseUtils.BrokerLookupResult> brokers = new ConcurrentHashMap<>();
        List<CompletableFuture<?>> lookups = new ArrayList<>(topicPartitionNum.get());
        fetchData.forEach((topicPartition, partitionData) -> {
            final String fullPartitionName = KopTopic.toString(topicPartition, namespacePrefix);
            lookups.add(findBroker(TopicName.get(fullPartitionName))
                    .thenAccept(p -> brokers.put(fullPartitionName, p)));
        });
        // here we can perform the last part of the processing in a separate thread
        // because one client won't perform many
        // fetch requests concurrently
        // without waiting for the results of the previous fetch
        FutureUtil.waitForAll(lookups)
                .whenComplete((result, error) -> {
                    // TODO: report errors for specific partitions and continue for non failed lookups
                    if (error != null) {
                        Map<TopicPartition, FetchResponse.PartitionData<?>> errorsMap =
                                fetchData
                                        .keySet()
                                        .stream()
                                        .collect(Collectors.toMap(Function.identity(),
                                                p -> new FetchResponse.PartitionData(Errors.UNKNOWN_SERVER_ERROR,
                                                        0, 0, 0,
                                                        null, MemoryRecords.EMPTY)));
                        resultFuture.complete(new FetchResponse(Errors.UNKNOWN_SERVER_ERROR,
                                new LinkedHashMap<>(errorsMap), 0, fetchRequest.metadata().sessionId()));
                    } else {
                        boolean multipleBrokers = false;

                        // check if all the partitions are for the same broker
                        KafkaResponseUtils.BrokerLookupResult first = null;
                        for (KafkaResponseUtils.BrokerLookupResult md : brokers.values()) {
                            if (first == null) {
                                first = md;
                            } else if (!Objects.equals(first.node, md.node)) {
                                multipleBrokers = true;
                                break;
                            }
                        }


                        if (!multipleBrokers) {
                            // all the partitions are owned by one single broker,
                            // we can forward the whole request to the only broker
                            log.debug("forward FULL fetch of {} parts to {}", numPartitions, first);
                            grabConnectionToBroker(first.node.host(), first.node.port()).
                                    forwardRequest(fetch)
                                    .thenAccept(response -> {
                                        resultFuture.complete(response);
                                    }).exceptionally(badError -> {
                                        log.error("bad error for FULL fetch", badError);
                                        Map<TopicPartition, FetchResponse.PartitionData<?>> errorsMap =
                                                fetchData
                                                        .keySet()
                                                        .stream()
                                                        .collect(Collectors.toMap(Function.identity(),
                                                                p -> new FetchResponse.PartitionData(
                                                                        Errors.UNKNOWN_SERVER_ERROR,
                                                                        0, 0, 0,
                                                                        null, MemoryRecords.EMPTY)));
                                        resultFuture.complete(new FetchResponse(Errors.UNKNOWN_SERVER_ERROR,
                                                new LinkedHashMap<>(errorsMap), 0,
                                                fetchRequest.metadata().sessionId()));
                                        return null;
                                    });
                        } else {
                            log.debug("Split fetch of {} parts to {}", numPartitions, brokers);
                            // we have to create multiple FetchRequest
                            // this is a prototype, let's create a FetchRequest per each partition
                            // we could group requests per broker

                            Runnable complete = () -> {
                                log.debug("complete fetch {}", fetch);
                                topicPartitionNum.set(0);
                                if (resultFuture.isDone()) {
                                    // It may be triggered again in DelayedProduceAndFetch
                                    return;
                                }
                                // add the topicPartition with timeout error if it's not existed in responseMap
                                fetchData.keySet().forEach(topicPartition -> {
                                    if (!responseMap.containsKey(topicPartition)) {
                                        responseMap.put(topicPartition,
                                                new FetchResponse.PartitionData(Errors.UNKNOWN_SERVER_ERROR,
                                                        0, 0, 0,
                                                        null, MemoryRecords.EMPTY));
                                    }
                                });
                                if (log.isDebugEnabled()) {
                                    log.debug("[{}] Request {}: Complete handle fetch.", ctx.channel(),
                                            fetch.toString());
                                }
                                final LinkedHashMap<TopicPartition, FetchResponse.PartitionData<?>> responseMapRaw =
                                        new LinkedHashMap<>(responseMap);
                                resultFuture.complete(new FetchResponse(Errors.NONE,
                                        responseMapRaw, 0, fetchRequest.metadata().sessionId()));
                            };
                            BiConsumer<TopicPartition, FetchResponse.PartitionData> addFetchPartitionResponse =
                                    (topicPartition, response) -> {

                                responseMap.put(topicPartition, response);
                                // reset topicPartitionNum
                                int restTopicPartitionNum = topicPartitionNum.decrementAndGet();
                                log.debug("addFetchPartitionResponse {} {} restTopicPartitionNum {}", topicPartition,
                                        response,
                                        restTopicPartitionNum);
                                if (restTopicPartitionNum < 0) {
                                    return;
                                }
                                if (restTopicPartitionNum == 0) {
                                    complete.run();
                                }
                            };

                            final BiConsumer<TopicPartition, FetchResponse.PartitionData> resultConsumer =
                                    (topicPartition, data) -> addFetchPartitionResponse.accept(
                                            topicPartition, data);
                            final BiConsumer<TopicPartition, Errors> errorsConsumer =
                                    (topicPartition, errors) -> addFetchPartitionResponse.accept(topicPartition,
                                            new FetchResponse.PartitionData(errors, 0, 0, 0,
                                                    null, MemoryRecords.EMPTY));

                            Map<Node, Map<TopicPartition, FetchRequest.PartitionData>> requestsByBroker =
                                                                                                new HashMap<>();

                            fetchData.forEach((topicPartition, partitionData) -> {
                                    final String fullPartitionName = KopTopic.toString(topicPartition, namespacePrefix);
                                    KafkaResponseUtils.BrokerLookupResult topicMetadata =
                                            brokers.get(fullPartitionName);
                                    Node kopBroker = topicMetadata.node;
                                    Map<TopicPartition, FetchRequest.PartitionData> requestForSinglePartition =
                                            requestsByBroker.computeIfAbsent(kopBroker, ___ -> new HashMap<>());
                                    requestForSinglePartition.put(topicPartition, partitionData);
                            });

                            requestsByBroker.forEach((kopBroker, requestsForBroker) -> {
                                int dummyCorrelationId = getDummyCorrelationId();
                                RequestHeader header = new RequestHeader(
                                        fetch.getHeader().apiKey(),
                                        fetch.getHeader().apiVersion(),
                                        fetch.getHeader().clientId(),
                                        dummyCorrelationId
                                );
                                FetchRequest requestForSingleBroker = FetchRequest.Builder
                                        .forConsumer(((FetchRequest) fetch.getRequest()).maxWait(),
                                                ((FetchRequest) fetch.getRequest()).minBytes(),
                                                requestsForBroker)
                                        .build();
                                ByteBuf buffer = KopResponseUtils.serializeRequest(header, requestForSingleBroker);
                                KafkaHeaderAndRequest singlePartitionRequest = new KafkaHeaderAndRequest(
                                        header,
                                        requestForSingleBroker,
                                        buffer,
                                        null
                                );
                                buffer.release();

                                if (log.isDebugEnabled()) {
                                    log.debug("forward fetch for {} to {}", requestForSingleBroker.fetchData().keySet(),
                                            kopBroker);
                                }
                                grabConnectionToBroker(kopBroker.host(), kopBroker.port())
                                        .forwardRequest(singlePartitionRequest)
                                        .thenAccept(response -> {
                                            FetchResponse<?> resp = (FetchResponse) response;
                                            resp.responseData()
                                                    .forEach((topicPartition, partitionResponse) -> {
                                                        invalidateLeaderIfNeeded(namespacePrefix, kopBroker,
                                                                topicPartition, partitionResponse.error());
                                                        if (log.isDebugEnabled()) {
                                                            final String fullPartitionName =
                                                                    KopTopic.toString(topicPartition,
                                                                            namespacePrefix);
                                                            log.debug("result fetch for {} to {} {}", fullPartitionName,
                                                                    kopBroker,
                                                                    partitionResponse);
                                                        }
                                                        addFetchPartitionResponse.accept(topicPartition,
                                                                partitionResponse);
                                                    });
                                        }).exceptionally(badError -> {
                                            log.error("bad error while fetching for {} from {}",
                                                    requestForSingleBroker.fetchData().keySet(), badError, kopBroker);
                                            requestForSingleBroker.fetchData().keySet().forEach(topicPartition ->
                                                    errorsConsumer.accept(topicPartition, Errors.UNKNOWN_SERVER_ERROR)
                                            );
                                            return null;
                                        }).whenComplete((ignore1, ignore2) -> {
                                            singlePartitionRequest.close();
                                        });
                            });
                        }
                    }
                });
    }

    private CompletableFuture<KafkaResponseUtils.BrokerLookupResult> findCoordinator(
            FindCoordinatorRequest.CoordinatorType type, String key) {
        String pulsarTopicName = computePulsarTopicName(type, key);
        log.debug("findCoordinator for {} {} -> topic {}", type, key, pulsarTopicName);
        if (coordinatorNamespaceExists) {
            return findBroker(TopicName.get(pulsarTopicName), true);
        } else {
            TopicName topicName = TopicName.get(pulsarTopicName);
            String nameSpace = topicName.getNamespace();
            return getPulsarAdmin(true)
                    .thenCompose(admin -> admin.namespaces().getNamespacesAsync(topicName.getTenant())
                            .thenCompose(namespaces -> {
                                if (namespaces.contains(nameSpace)) {
                                    coordinatorNamespaceExists = true;
                                    return CompletableFuture.completedFuture(null);
                                } else {
                                    log.debug("findCoordinator for {} {} -> topic {} -> CREATING NAMESPACE {}", type,
                                            key, pulsarTopicName, nameSpace);
                                    return admin.namespaces().createNamespaceAsync(nameSpace);
                                }
                            })
                            .thenCompose(___ -> {
                                coordinatorNamespaceExists = true;
                                return findBroker(TopicName.get(pulsarTopicName), true);
                            }));
        }
    }

    private String computePulsarTopicName(FindCoordinatorRequest.CoordinatorType type, String key) {
        String pulsarTopicName;
        int partition;
        String tenant = getCurrentTenant();
        if (type == FindCoordinatorRequest.CoordinatorType.TRANSACTION) {
            TransactionConfig transactionConfig = TransactionConfig.builder()
                    .transactionLogNumPartitions(kafkaConfig.getKafkaTxnLogTopicNumPartitions())
                    .transactionMetadataTopicName(MetadataUtils.constructTxnLogTopicBaseName(tenant, kafkaConfig))
                    .build();
            partition = TransactionCoordinator.partitionFor(key, kafkaConfig.getKafkaTxnLogTopicNumPartitions());
            pulsarTopicName = TransactionCoordinator
                    .getTopicPartitionName(transactionConfig.getTransactionMetadataTopicName(), partition);
        } else if (type == FindCoordinatorRequest.CoordinatorType.GROUP) {
            partition = GroupMetadataManager.getPartitionId(key, kafkaConfig.getOffsetsTopicNumPartitions());
            pulsarTopicName = GroupMetadataManager
                    .getTopicPartitionName(tenant + "/"
                            + kafkaConfig.getKafkaMetadataNamespace()
                            + "/" + Topic.GROUP_METADATA_TOPIC_NAME, partition);
        } else {
            throw new NotImplementedException("FindCoordinatorRequest not support TRANSACTION type " + type);
        }
        return pulsarTopicName;
    }

    protected void handleFindCoordinatorRequest(KafkaHeaderAndRequest findCoordinator,
                                                CompletableFuture<AbstractResponse> resultFuture) {
        Node node = newSelfNode();
        AbstractResponse response = new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setHost(node.host())
                    .setPort(node.port())
                    .setNodeId(node.id()));
        resultFuture.complete(response);
    }

    protected void handleJoinGroupRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                JoinGroupRequest.class,
                JoinGroupRequestData.class,
                JoinGroupRequestData::groupId,
                null);
    }

    protected void handleSyncGroupRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                SyncGroupRequest.class,
                SyncGroupRequestData.class,
                SyncGroupRequestData::groupId,
                null);
    }

    protected void handleHeartbeatRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                HeartbeatRequest.class,
                HeartbeatRequestData.class,
                HeartbeatRequestData::groupId,
                null);
    }

    @Override
    protected void handleLeaveGroupRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                           CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                LeaveGroupRequest.class,
                LeaveGroupRequestData.class,
                LeaveGroupRequestData::groupId,
                null);
    }

    @Override
    protected void handleDescribeGroupRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                              CompletableFuture<AbstractResponse> resultFuture) {
        // forward to the coordinator of the first group
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                DescribeGroupsRequest.class,
                DescribeGroupsRequestData.class,
                (DescribeGroupsRequestData r) -> r.groups().get(0),
                null);
    }

    @Override
    protected void handleListGroupsRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                           CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                ListGroupsRequest.class,
                ListGroupsRequestData.class,
                (metadataRequest) -> "system",
                null);
    }

    @Override
    protected void handleDeleteGroupsRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                             CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                DeleteGroupsRequest.class,
                DeleteGroupsRequestData.class,
                (metadataRequest) -> "system",
                null);
    }

    @Override
    protected void handleSaslAuthenticate(KafkaHeaderAndRequest saslAuthenticate,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        resultFuture.complete(new SaslAuthenticateResponse(
                new SaslAuthenticateResponseData()
                        .setErrorCode(Errors.ILLEGAL_SASL_STATE.code())
                        .setErrorMessage("SaslAuthenticate request received after successful authentication")));
    }

    @Override
    protected void handleSaslHandshake(KafkaHeaderAndRequest saslHandshake,
                                       CompletableFuture<AbstractResponse> resultFuture) {
        resultFuture.complete(new SaslHandshakeResponse(
                new SaslHandshakeResponseData()
                        .setErrorCode(Errors.ILLEGAL_SASL_STATE.code())
                        .setMechanisms(Collections.emptyList())));
    }

    @Override
    protected void handleCreateTopics(KafkaHeaderAndRequest createTopics,
                                      CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(createTopics, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                CreateTopicsRequest.class,
                CreateTopicsRequestData.class,
                (metadataRequest) -> "system",
                null);
    }

    @Override
    protected void handleCreatePartitions(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                CreatePartitionsRequest.class,
                CreatePartitionsRequestData.class,
                (metadataRequest) -> "system",
                null);
    }

    protected void handleDescribeConfigs(KafkaHeaderAndRequest describeConfigs,
                                         CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(describeConfigs, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                DescribeConfigsRequest.class,
                DescribeConfigsRequestData.class,
                (metadataRequest) -> "system",
                null);
    }

    protected void handleAlterConfigs(KafkaHeaderAndRequest describeConfigs,
                                      CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(describeConfigs, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                AlterConfigsRequest.class,
                AlterConfigsRequestData.class,
                (metadataRequest) -> "system",
                null);
    }

    @Override
    protected void handleDeleteTopics(KafkaHeaderAndRequest deleteTopics,
                                      CompletableFuture<AbstractResponse> resultFuture) {

        DeleteTopicsRequest request = (DeleteTopicsRequest) deleteTopics.getRequest();
        if (request.topics().isEmpty()) {
            resultFuture.complete(new DeleteTopicsResponse(
                    new DeleteTopicsResponseData()
            ));
            return;
        }
        String namespacePrefix = currentNamespacePrefix();
        DeleteTopicsRequestData data = request.data();
        List<DeleteTopicsRequestData.DeleteTopicState> topics = data.topics();
        Map<String, Errors> responseMap = new ConcurrentHashMap<>();
        int numTopics = topics.size();
        Map<String, KafkaResponseUtils.BrokerLookupResult> brokers = new ConcurrentHashMap<>();
        List<CompletableFuture<?>> lookups = new ArrayList<>(numTopics);
        topics.forEach((topic) -> {
            KopTopic kopTopic;
            try {
                kopTopic = new KopTopic(topic.name(), namespacePrefix);
            } catch (KoPTopicException var6) {
                responseMap.put(topic.name(), Errors.UNKNOWN_TOPIC_OR_PARTITION);
                return;
            }
            final String fullPartitionName = kopTopic.getFullName();
            lookups.add(findBroker(TopicName.get(fullPartitionName))
                    .thenAccept(p -> brokers.put(fullPartitionName, p)));
        });
        //TODO: split the request

        handleRequestWithCoordinator(deleteTopics, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                DeleteTopicsRequest.class,
                DeleteTopicsRequestData.class,
                (metadataRequest) -> "system",
                null);
    }

    @Override
    protected void handleDeleteRecords(KafkaHeaderAndRequest deleteRecords,
                                       CompletableFuture<AbstractResponse> resultFuture) {

        DeleteRecordsRequest request = (DeleteRecordsRequest) deleteRecords.getRequest();
        DeleteRecordsRequestData data = request.data();
        Map<TopicPartition, Long> partitionOffsets = new HashMap<>();
        data.topics().forEach(topic -> {
            topic.partitions().forEach(partition -> {
                partitionOffsets.put(new TopicPartition(topic.name(), partition.partitionIndex()),
                        partition.offset());
            });
        });
        if (partitionOffsets.isEmpty()) {
            resultFuture.complete(KafkaResponseUtils.newDeleteRecords(Collections.emptyMap()));
            return;
        }
        String namespacePrefix = currentNamespacePrefix();

        Map<TopicPartition, Errors> responseMap = new ConcurrentHashMap<>();
        int numPartitions = partitionOffsets.size();
        Map<String, KafkaResponseUtils.BrokerLookupResult> brokers = new ConcurrentHashMap<>();
        List<CompletableFuture<?>> lookups = new ArrayList<>(numPartitions);
        partitionOffsets.forEach((topicPartition, offset) -> {
            final String fullPartitionName = KopTopic.toString(topicPartition,
                    namespacePrefix);
            lookups.add(findBroker(TopicName.get(fullPartitionName))
                    .thenAccept(p -> brokers.put(fullPartitionName, p)));
        });
        AtomicInteger topicPartitionNum = new AtomicInteger(numPartitions);

        // here we can perform the last part of the processing in a separate thread
        // because one client usually won't perform many
        // deleteRequests concurrently
        // without waiting for the results of the previous delete
        FutureUtil.waitForAll(lookups)
                .whenComplete((result, error) -> {
                    // TODO: report errors for specific partitions and continue for non failed lookups
                    if (error != null) {
                        Map<TopicPartition, Errors> errorsMap =
                                partitionOffsets
                                        .keySet()
                                        .stream()
                                        .collect(Collectors.toMap(Function.identity(),
                                                p ->  Errors.UNKNOWN_SERVER_ERROR));
                        resultFuture.complete(KafkaResponseUtils.newDeleteRecords(errorsMap));
                    } else {
                        boolean multipleBrokers = false;

                        // check if all the partitions are for the same broker
                        KafkaResponseUtils.BrokerLookupResult first = null;
                        for (KafkaResponseUtils.BrokerLookupResult md : brokers.values()) {
                            if (first == null) {
                                first = md;
                            } else if (!Objects.equals(first.node, md.node)) {
                                multipleBrokers = true;
                                break;
                            }
                        }


                        if (!multipleBrokers) {
                            // all the partitions are owned by one single broker,
                            // we can forward the whole request to the only broker
                            log.debug("forward FULL DeleteRecords of {} parts to {}", numPartitions, first);
                            grabConnectionToBroker(first.node.host(), first.node.port()).
                                    forwardRequest(deleteRecords)
                                    .thenAccept(response -> {
                                        resultFuture.complete(response);
                                    }).exceptionally(badError -> {
                                        log.error("bad error for FULL DeleteRecords", badError);
                                        Map<TopicPartition, Errors> errorsMap =
                                                partitionOffsets
                                                        .keySet()
                                                        .stream()
                                                        .collect(Collectors.toMap(Function.identity(),
                                                                p -> Errors.UNKNOWN_SERVER_ERROR));
                                        resultFuture.complete(KafkaResponseUtils.newDeleteRecords(errorsMap));
                                        return null;
                                    });
                        } else {
                            log.debug("Split DeleteRecords of {} parts to {}", numPartitions, brokers);
                            // we have to create multiple FetchRequest
                            // this is a prototype, let's create a FetchRequest per each partition
                            // we could group requests per broker

                            Runnable complete = () -> {
                                log.debug("complete fetch {}", deleteRecords);
                                topicPartitionNum.set(0);
                                if (resultFuture.isDone()) {
                                    // It may be triggered again in DelayedProduceAndFetch
                                    return;
                                }
                                // add the topicPartition with timeout error if it's not existed in responseMap
                                partitionOffsets.keySet().forEach(topicPartition -> {
                                    if (!responseMap.containsKey(topicPartition)) {
                                        responseMap.put(topicPartition, Errors.UNKNOWN_SERVER_ERROR);
                                    }
                                });
                                if (log.isDebugEnabled()) {
                                    log.debug("[{}] Request {}: Complete handle DeleteRecords.", ctx.channel(),
                                            deleteRecords);
                                }
                                final LinkedHashMap<TopicPartition, Errors>
                                        responseMapRaw =
                                        new LinkedHashMap<>(responseMap);
                                resultFuture.complete(KafkaResponseUtils.newDeleteRecords(responseMapRaw));
                            };
                            BiConsumer<TopicPartition, Errors>
                                    addDeletePartitionResponse = (topicPartition, response) -> {

                                responseMap.put(topicPartition, response);
                                // reset topicPartitionNum
                                int restTopicPartitionNum = topicPartitionNum.decrementAndGet();
                                log.debug("addDeletePartitionResponse {} {} restTopicPartitionNum {}", topicPartition,
                                        response,
                                        restTopicPartitionNum);
                                if (restTopicPartitionNum < 0) {
                                    return;
                                }
                                if (restTopicPartitionNum == 0) {
                                    complete.run();
                                }
                            };

                            final BiConsumer<TopicPartition, Errors> resultConsumer =
                                    (topicPartition, data2) -> addDeletePartitionResponse.accept(
                                            topicPartition, data2);
                            final BiConsumer<TopicPartition, Errors> errorsConsumer =
                                    (topicPartition, errors) -> addDeletePartitionResponse.accept(topicPartition,
                                            errors);

                            Map<Node, DeleteRecordsRequestData> requestsByBroker = new HashMap<>();

                            partitionOffsets.forEach((topicPartition, offset) -> {
                                final String fullPartitionName = KopTopic.toString(topicPartition, namespacePrefix);
                                KafkaResponseUtils.BrokerLookupResult topicMetadata = brokers.get(fullPartitionName);
                                Node kopBroker = topicMetadata.node;
                                DeleteRecordsRequestData requestForSinglePartition = requestsByBroker
                                        .computeIfAbsent(kopBroker, ___ -> new DeleteRecordsRequestData()
                                                .setTimeoutMs(data.timeoutMs()));

                                DeleteRecordsRequestData.DeleteRecordsTopic deleteRecordsTopic =
                                        requestForSinglePartition.topics().stream()
                                        .filter(t -> t.name().equals(topicPartition.topic()))
                                        .findFirst().orElse(null);
                                if (deleteRecordsTopic == null) {
                                    deleteRecordsTopic = new DeleteRecordsRequestData.DeleteRecordsTopic()
                                            .setName(topicPartition.topic());
                                    requestForSinglePartition.topics().add(deleteRecordsTopic);
                                }
                                deleteRecordsTopic.partitions()
                                        .add(new DeleteRecordsRequestData.DeleteRecordsPartition()
                                        .setPartitionIndex(topicPartition.partition())
                                        .setOffset(offset));
                            });

                            requestsByBroker.forEach((kopBroker, requestForSingleBroker) -> {
                                int dummyCorrelationId = getDummyCorrelationId();
                                RequestHeader header = new RequestHeader(
                                        deleteRecords.getHeader().apiKey(),
                                        deleteRecords.getHeader().apiVersion(),
                                        deleteRecords.getHeader().clientId(),
                                        dummyCorrelationId
                                );
                                DeleteRecordsRequest requestForBroker =
                                        new DeleteRecordsRequest.Builder(requestForSingleBroker)
                                        .build(request.version());
                                ByteBuf buffer = KopResponseUtils.serializeRequest(header, requestForBroker);
                                KafkaHeaderAndRequest singlePartitionRequest = new KafkaHeaderAndRequest(
                                        header,
                                        requestForBroker,
                                        buffer,
                                        null
                                );
                                buffer.release();

                                if (log.isDebugEnabled()) {
                                    log.debug("forward DeleteRequest for {} to {}",
                                            requestForSingleBroker
                                                    .topics()
                                                    .stream()
                                                    .map(t -> t.name())
                                                    .collect(Collectors.toList()), kopBroker);
                                }
                                grabConnectionToBroker(kopBroker.host(), kopBroker.port())
                                        .forwardRequest(singlePartitionRequest)
                                        .thenAccept(response -> {
                                            DeleteRecordsResponse resp = (DeleteRecordsResponse) response;
                                            resp.data().topics().forEach(topic -> {
                                                topic.partitions()
                                                    .forEach((partitionResponse) -> {
                                                        TopicPartition topicPartition = new TopicPartition(topic.name(),
                                                                partitionResponse.partitionIndex());
                                                    invalidateLeaderIfNeeded(namespacePrefix, kopBroker,
                                                            topicPartition, partitionResponse.errorCode());
                                                    if (log.isDebugEnabled()) {
                                                        final String fullPartitionName =
                                                                KopTopic.toString(topicPartition,
                                                                        namespacePrefix);
                                                        log.debug("result fetch for {} to {} {}", fullPartitionName,
                                                                kopBroker,
                                                                partitionResponse);
                                                    }
                                                    addDeletePartitionResponse.accept(topicPartition,
                                                            Errors.forCode(partitionResponse.errorCode()));
                                                });
                                            });
                                        }).exceptionally(badError -> {
                                            log.error("bad error while fetching for {} from {}",
                                                    requestForSingleBroker
                                                            .topics()
                                                            .stream()
                                                            .map(t -> t.name())
                                                            .collect(Collectors.toList()), badError,
                                                    kopBroker);
                                            requestForSingleBroker.topics().forEach(topic -> {
                                                topic.partitions().forEach(partition -> {
                                                    TopicPartition topicPartition =
                                                        new TopicPartition(topic.name(), partition.partitionIndex());
                                                    errorsConsumer.accept(topicPartition, Errors.UNKNOWN_SERVER_ERROR);
                                                });
                                            });
                                            return null;
                                        }).whenComplete((ignore1, ignore2) -> {
                                            singlePartitionRequest.close();
                                        });
                            });
                        }
                    }
                });

    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Caught error in handler, closing channel", cause);
        this.close();
    }

    private CompletableFuture<Optional<String>> getProtocolDataToAdvertise(String pulsarAddress,
                                                                           TopicName topic) {

        CompletableFuture<Optional<String>> returnFuture = new CompletableFuture<>();

        if (pulsarAddress == null) {
            log.error("[{}] failed get pulsar address, returned null.", topic.toString());

            returnFuture.complete(Optional.empty());
            return returnFuture;
        }

        // the Mapping to the KOP port is done per-convention
        // this saves us from a Metadata lookup hop
        String kafkaAddress = brokerAddressMapper.apply(pulsarAddress);

        log.debug("Found broker for topic {} pulsarAddress: {} kafkaAddress {}",
                topic, pulsarAddress, kafkaAddress);

        return CompletableFuture.completedFuture(Optional.of(kafkaAddress));
    }

    String currentUser() {
        if (authenticator != null
                && authenticator.session() != null
                && authenticator.session().getPrincipal() != null) {
            return authenticator.session().getPrincipal().getName();
        } else {
            return null;
        }
    }

    String getCurrentTenant() {
        return getCurrentTenant(kafkaConfig.getKafkaMetadataTenant());
    }

    String getCurrentTenant(String defaultTenant) {
        if (kafkaConfig.isKafkaEnableMultiTenantMetadata()
                && authenticator != null
                && authenticator.session() != null
                && authenticator.session().getPrincipal() != null
                && authenticator.session().getPrincipal().getTenantSpec() != null) {
            String tenantSpec = authenticator.session().getPrincipal().getTenantSpec();
            return extractTenantFromTenantSpec(tenantSpec);
        }
        // fallback to using system (default) tenant
        return defaultTenant;
    }

    private String currentNamespacePrefix() {
        String currentTenant = getCurrentTenant(kafkaConfig.getKafkaTenant());
        return MetadataUtils.constructUserTopicsNamespace(currentTenant, kafkaConfig);
    }

    private CompletableFuture<PulsarAdmin> getPulsarAdmin(boolean system) {
        try {
            String principal = currentUser();
            if (principal != null && system && !StringUtils.isBlank(kafkaConfig.getKafkaProxySuperUserRole())) {
                // sometimes we need a super user to perform some system operations,
                // like for finding coordinators
                // but if you are not authenticated (principal = null) then we do not use this power in any case
                principal = kafkaConfig.getKafkaProxySuperUserRole();
            }
            return CompletableFuture.completedFuture(admin.getAdminForPrincipal(principal));
        } catch (Exception err) {
            return FutureUtil.failedFuture(err);
        }
    }

    public CompletableFuture<KafkaResponseUtils.BrokerLookupResult> findBroker(TopicName topic) {
        return findBroker(topic, false);
    }

    public CompletableFuture<KafkaResponseUtils.BrokerLookupResult> findBroker(TopicName topic, boolean system) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Handle Lookup for {}", ctx.channel(), topic);
        }
        CompletableFuture<KafkaResponseUtils.BrokerLookupResult> returnFuture = new CompletableFuture<>();

        Node cached = topicsLeaders.get(topic.toString());

        if (cached != null) {
            returnFuture.complete(newPartitionMetadata(topic, cached));
            return returnFuture;
        }
        final AtomicReference<PulsarAdmin> pulsarAdminAtomicReference = new AtomicReference<>();
        getPulsarAdmin(system)
                .thenCompose(admin -> {
                    pulsarAdminAtomicReference.set(admin);
                    return admin.lookups().lookupTopicAsync(topic.toString());
                }).thenCompose(address -> getProtocolDataToAdvertise(address, topic))
                .whenComplete((stringOptional, throwable) -> {
                    if (throwable != null) {
                        invalidateCurrentPulsarAdminForError(pulsarAdminAtomicReference.get(), throwable, system);
                    }
                    if (throwable != null || stringOptional == null || !stringOptional.isPresent()) {
                        log.error("Not get advertise data for Kafka topic:{}. throwable",
                                topic, throwable);
                        returnFuture.complete(null);
                        return;
                    }

                    // It's the `kafkaAdvertisedListeners` config that's written to ZK
                    final String listeners = stringOptional.get();
                    // here we always connect in pleintext to the Pulsar broker
                    final EndPoint endPoint = kafkaConfig.isKopTlsEnabledWithBroker()
                            ? EndPoint.getSslEndPoint(listeners) : EndPoint.getPlainTextEndPoint(listeners);
                    final Node node = newNode(endPoint.getInetAddress());

                    if (log.isTraceEnabled()) {
                        log.trace("Found broker localListeners: {} for topicName: {}, "
                                        + "localListeners: {}, found Listeners: {}",
                                listeners, topic, advertisedListeners, listeners);
                    }

                    String fullTopicName = topic.toString();
                    topicsLeaders.put(fullTopicName, node);
                    log.info("found leader for {}: {}", fullTopicName, node);
                    returnFuture.complete(newPartitionMetadata(topic, node));
                }).exceptionally(error -> {
                    log.error("bad error for findBroker for topic {}", topic, error);
                    invalidateCurrentPulsarAdminForError(pulsarAdminAtomicReference.get(), error, system);
                    returnFuture.completeExceptionally(error);
                    return Optional.empty();
                });
        return returnFuture;
    }

    private void invalidateCurrentPulsarAdminForError(PulsarAdmin current, Throwable error, boolean system) {
        String principal = currentUser();
        if (principal != null && system && !StringUtils.isBlank(kafkaConfig.getKafkaProxySuperUserRole())) {
            // sometimes we need a super user to perform some system operations,
            // like for finding coordinators
            // but if you are not authenticated (principal = null) then we do not use this power in any case
            principal = kafkaConfig.getKafkaProxySuperUserRole();
        }
        log.info("invalidateCurrentPulsarAdminForError {} {} {}", principal, system, error);
        admin.invalidateAdminForPrincipal(principal, current, error);
    }

    Node newSelfNode() {
        return newNode(advertisedEndPoint.getInetAddress());
    }

    protected CompletableFuture<Boolean> authorize(AclOperation operation, Resource resource, Session session) {
        return KafkaRequestHandler.authorize(operation, resource, session, authorizer);
    }

    @Override
    protected void handleListOffsetRequest(KafkaHeaderAndRequest listOffset,
                                           CompletableFuture<AbstractResponse> resultFuture) {
        if (listOffset.getHeader().apiVersion() == 0) {
            // clients up to Kafka 0.10.0.0
            handleListOffsetRequestV0(listOffset, resultFuture);
        } else {
            handleListOffsetRequestV1(listOffset, resultFuture);
        }
    }

    protected void handleListOffsetRequestV0(KafkaHeaderAndRequest listOffset,
                                             CompletableFuture<AbstractResponse> resultFuture) {
// use offsetData
        ListOffsetRequestV0 request =
                byteBufToListOffsetRequestV0(listOffset.getBuffer());

        Map<TopicPartition, ListOffsetsResponseData.ListOffsetsPartitionResponse> map = new ConcurrentHashMap<>();
        AtomicInteger expectedCount = new AtomicInteger(request.offsetData().size());

        BiConsumer<String, ListOffsetsResponseData> onResponse = (topic, topicResponse) -> {
            topicResponse.topics().forEach(topicResp -> {
                topicResp.partitions().forEach(listOffsetsPartitionResponse -> {
                    TopicPartition tp = new TopicPartition(topicResp.name(),
                            listOffsetsPartitionResponse.partitionIndex());
                    map.put(tp, listOffsetsPartitionResponse);
                    if (expectedCount.decrementAndGet() == 0) {
                        ListOffsetsResponseData responseData = new ListOffsetsResponseData();
                        map.forEach((topicPartition, listOffsetsPartitionResponse1) -> {
                            ListOffsetsResponseData.ListOffsetsTopicResponse topicResponseData =
                                    responseData.topics().stream()
                                    .filter(t -> t.name().equals(topicPartition.topic()))
                                    .findFirst().orElse(null);
                            if (topicResponseData == null) {
                                topicResponseData = new ListOffsetsResponseData.ListOffsetsTopicResponse()
                                        .setName(topicPartition.topic());
                                responseData.topics().add(topicResponseData);
                            }
                            topicResponseData.partitions()
                                    .add(new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                                    .setPartitionIndex(topicPartition.partition())
                                    .setOffset(listOffsetsPartitionResponse1.offset())
                                    .setErrorCode(listOffsetsPartitionResponse1.errorCode())
                                    .setLeaderEpoch(listOffsetsPartitionResponse1.leaderEpoch())
                                    .setTimestamp(listOffsetsPartitionResponse1.timestamp())
                                    .setOldStyleOffsets(listOffsetsPartitionResponse1.oldStyleOffsets()));
                        });
                        ListOffsetsResponse response = new ListOffsetsResponse(responseData);
                        resultFuture.complete(response);
                    }
                });
            });
        };
        String namespacePrefix = currentNamespacePrefix();
        for (Map.Entry<TopicPartition, ListOffsetRequestV0.PartitionData> entry : request.offsetData().entrySet()) {
            final String fullPartitionName = KopTopic.toString(entry.getKey(), namespacePrefix);

            int dummyCorrelationId = getDummyCorrelationId();
            RequestHeader header = new RequestHeader(
                    listOffset.getHeader().apiKey(),
                    listOffset.getHeader().apiVersion(),
                    listOffset.getHeader().clientId(),
                    dummyCorrelationId
            );

            Map<TopicPartition, ListOffsetRequestV0.PartitionData> tsData = new HashMap<>();
            tsData.put(entry.getKey(), entry.getValue());
            ListOffsetRequestV0 requestForSinglePartition = ListOffsetRequestV0.Builder
                    .forConsumer(false, request.isolationLevel())
                    .setOffsetData(tsData)
                    .build(request.version());

            findBroker(TopicName.get(fullPartitionName))
                    .thenAccept(brokerAddress -> {
                        ByteBuf buffer = KopResponseUtils.serializeRequest(header, requestForSinglePartition);

                        KafkaHeaderAndRequest singlePartitionRequest = new KafkaHeaderAndRequest(
                                header,
                                requestForSinglePartition,
                                buffer,
                                null
                        );
                        buffer.release();
                        grabConnectionToBroker(brokerAddress.node.host(), brokerAddress.node.port())
                                .forwardRequest(singlePartitionRequest)
                                .thenAccept(theResponse -> {
                                    onResponse.accept(fullPartitionName, (ListOffsetsResponseData) theResponse.data());
                                }).exceptionally(err -> {
                                    ListOffsetsResponseData responseData =
                                            buildDummyListOffsetsResponseData(entry.getKey());
                                    onResponse.accept(fullPartitionName, responseData);
                                    return null;
                                }).whenComplete((ignore1, ignore2) -> {
                                    singlePartitionRequest.close();
                                });
                    }).exceptionally(err -> {
                        ListOffsetsResponseData responseData = buildDummyListOffsetsResponseData(entry.getKey());
                        onResponse.accept(fullPartitionName, responseData);
                        return null;
                    });
        }
    }

    private static ListOffsetsResponseData buildDummyListOffsetsResponseData(
            TopicPartition tp) {
        ListOffsetsResponseData responseData = new ListOffsetsResponseData();
        ListOffsetsResponseData.ListOffsetsTopicResponse listOffsetsTopicResponse =
                new ListOffsetsResponseData.ListOffsetsTopicResponse()
                .setName(tp.topic());
        responseData.topics().add(listOffsetsTopicResponse);
        listOffsetsTopicResponse.partitions()
                .add(new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                .setErrorCode(Errors.BROKER_NOT_AVAILABLE.code())
                .setPartitionIndex(tp.partition()));
        return responseData;
    }

    protected void handleListOffsetRequestV1(KafkaHeaderAndRequest listOffset,
                                             CompletableFuture<AbstractResponse> resultFuture) {
        // use partitionTimestamps
        checkArgument(listOffset.getRequest() instanceof ListOffsetsRequest);
        ListOffsetsRequest request = (ListOffsetsRequest) listOffset.getRequest();
        ListOffsetsRequestData data = request.data();

        if (data.topics().isEmpty()) {
            // this should not happen
            ListOffsetsResponse response = new ListOffsetsResponse(new ListOffsetsResponseData());
            resultFuture.complete(response);
            return;
        }

        Map<TopicPartition, ListOffsetsResponseData.ListOffsetsPartitionResponse> map = new ConcurrentHashMap<>();
        AtomicInteger expectedCount = new AtomicInteger(request
                .topics()
                .stream().mapToInt(t -> t.partitions().size())
                .sum());

        BiConsumer<String, ListOffsetsResponseData> onResponse = (topic, topicResponse) -> {
            topicResponse.topics().forEach(topicResp -> {
                topicResp.partitions().forEach(listOffsetsPartitionResponse -> {
                    TopicPartition tp = new TopicPartition(topicResp.name(),
                            listOffsetsPartitionResponse.partitionIndex());
                    map.put(tp, listOffsetsPartitionResponse);
                    if (expectedCount.decrementAndGet() == 0) {
                        ListOffsetsResponseData responseData = new ListOffsetsResponseData();
                        map.forEach((topicPartition, listOffsetsPartitionResponse1) -> {
                            ListOffsetsResponseData.ListOffsetsTopicResponse topicResponseData =
                                    responseData.topics().stream()
                                    .filter(t -> t.name().equals(topicPartition.topic()))
                                    .findFirst().orElse(null);
                            if (topicResponseData == null) {
                                topicResponseData = new ListOffsetsResponseData.ListOffsetsTopicResponse()
                                        .setName(topicPartition.topic());
                                responseData.topics().add(topicResponseData);
                            }
                            topicResponseData.partitions()
                                    .add(new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                                    .setPartitionIndex(topicPartition.partition())
                                    .setOffset(listOffsetsPartitionResponse1.offset())
                                    .setErrorCode(listOffsetsPartitionResponse1.errorCode())
                                    .setLeaderEpoch(listOffsetsPartitionResponse1.leaderEpoch())
                                    .setTimestamp(listOffsetsPartitionResponse1.timestamp())
                                    .setOldStyleOffsets(listOffsetsPartitionResponse1.oldStyleOffsets()));
                        });
                        ListOffsetsResponse response = new ListOffsetsResponse(responseData);
                        resultFuture.complete(response);
                    }
                });
            });
        };


        String namespacePrefix = currentNamespacePrefix();
        request.topics().forEach(topic -> {
            topic.partitions().forEach(listOffsetsPartition -> {
                TopicPartition topicPartition = new TopicPartition(topic.name(), listOffsetsPartition.partitionIndex());


            final String fullPartitionName = KopTopic.toString(topicPartition, namespacePrefix);

            int dummyCorrelationId = getDummyCorrelationId();
            RequestHeader header = new RequestHeader(
                    listOffset.getHeader().apiKey(),
                    listOffset.getHeader().apiVersion(),
                    listOffset.getHeader().clientId(),
                    dummyCorrelationId
            );

            ListOffsetsRequestData.ListOffsetsTopic tsData =
                    new ListOffsetsRequestData.ListOffsetsTopic()
                            .setName(topic.name());
            tsData.partitions().add(new ListOffsetsRequestData.ListOffsetsPartition()
                    .setPartitionIndex(topicPartition.partition())
                    .setTimestamp(listOffsetsPartition.timestamp())
                    .setCurrentLeaderEpoch(listOffsetsPartition.currentLeaderEpoch())
                    .setMaxNumOffsets(listOffsetsPartition.maxNumOffsets()));

            ListOffsetsRequest requestForSinglePartition = ListOffsetsRequest.Builder
                    .forConsumer(false, request.isolationLevel())
                    .setTargetTimes(Collections.singletonList(tsData))
                    .build(request.version());

            findBroker(TopicName.get(fullPartitionName))
                    .thenAccept(brokerAddress -> {
                        ByteBuf buffer = KopResponseUtils.serializeRequest(header, requestForSinglePartition);
                        KafkaHeaderAndRequest singlePartitionRequest = new KafkaHeaderAndRequest(
                                header,
                                requestForSinglePartition,
                                buffer,
                                null
                        );
                        buffer.release();
                        grabConnectionToBroker(brokerAddress.node.host(), brokerAddress.node.port())
                                .forwardRequest(singlePartitionRequest)
                                .thenAccept(theResponse -> {
                                    onResponse.accept(fullPartitionName, ((ListOffsetsResponse) theResponse).data());
                                }).exceptionally(err -> {
                                    ListOffsetsResponseData responseData =
                                            buildDummyListOffsetsResponseData(topicPartition);
                                    onResponse.accept(fullPartitionName, responseData);
                                    return null;
                                }).whenComplete((ignore1, ignore2) -> {
                                    singlePartitionRequest.close();
                                });
                    }).exceptionally(err -> {
                        ListOffsetsResponseData responseData = buildDummyListOffsetsResponseData(topicPartition);
                        onResponse.accept(fullPartitionName, responseData);
                        return null;
                    });
            });
        });
    }

    @Override
    protected void handleOffsetFetchRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                            CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                OffsetFetchRequest.class,
                OffsetFetchRequestData.class,
                OffsetFetchRequestData::groupId,
                null);
    }

    @Override
    protected void handleOffsetCommitRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                             CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture,
                FindCoordinatorRequest.CoordinatorType.GROUP,
                OffsetCommitRequest.class,
                OffsetCommitRequestData.class,
                OffsetCommitRequestData::groupId,
                null);
    }

    private <K extends AbstractRequest,
            V extends ApiMessage,
            R extends AbstractResponse> void handleRequestWithCoordinator(
            KafkaHeaderAndRequest kafkaHeaderAndRequest,
            CompletableFuture<AbstractResponse> resultFuture,
            FindCoordinatorRequest.CoordinatorType coordinatorType,
            Class<K> requestClass,
            Class<V> requestDataClass,
            Function<V, String> keyExtractor,
            BiFunction<K, Throwable, R> customErrorBuilder
    ) {
        BiFunction<K, Throwable, R> errorBuilder;
        if (customErrorBuilder == null) {
            errorBuilder = (K request, Throwable t) -> {
                if (t instanceof IOException
                    || t.getCause() instanceof IOException) {
                    t = new CoordinatorNotAvailableException("Network error: " + t, t);
                }
                return (R) request.getErrorResponse(t);
            };
        } else {
            errorBuilder = customErrorBuilder;
        }
        try {
            checkArgument(requestClass.isInstance(kafkaHeaderAndRequest.getRequest()));
            K request = (K) kafkaHeaderAndRequest.getRequest();
            checkArgument(requestDataClass.isInstance(request.data()));
            V data = (V) request.data();
            String transactionalId = keyExtractor.apply(data);
            if (!isNoisyRequest(request)) {
                log.info("handleRequestWithCoordinator {} {} {} {}", request.getClass().getSimpleName(), request,
                        transactionalId);
            }

            findCoordinator(coordinatorType, transactionalId)
                    .thenAccept(metadata -> {
                        grabConnectionToBroker(metadata.node.host(), metadata.node.port())
                                .forwardRequest(kafkaHeaderAndRequest)
                                .thenAccept(serverResponse -> {
                                    if (!isNoisyRequest(request)) {
                                        log.info("Sending {} {} from {}:{} errors {}.", serverResponse,
                                                serverResponse.getClass(),
                                                metadata.node.host(), metadata.node.port(),
                                                serverResponse.errorCounts());
                                    }
                                    if (serverResponse.errorCounts() != null) {
                                        for (Errors error : serverResponse.errorCounts().keySet()) {
                                            if (error == Errors.NOT_COORDINATOR
                                                    || error == Errors.NOT_CONTROLLER
                                                    || error == Errors.COORDINATOR_NOT_AVAILABLE
                                                    || error == Errors.NOT_LEADER_OR_FOLLOWER) {
                                                forgetMetadataForFailedBroker(metadata.node.host(),
                                                        metadata.node.port());
                                            }
                                        }
                                    }
                                    resultFuture.complete(serverResponse);
                                }).exceptionally(err -> {
                                    log.error("Error sending {} coordinator for id {} request to {} :{}",
                                            coordinatorType, transactionalId, metadata.node, err);
                                    resultFuture.complete(errorBuilder.apply(request, err));
                                    return null;
                                });
                    })
                    .exceptionally((err) -> {
                        resultFuture.complete(errorBuilder.apply(request, err));
                        return null;
                    });
        } catch (RuntimeException err) {
            log.error("Runtime error " + err, err);
            resultFuture.completeExceptionally(err);
        }
    }

    private <R extends AbstractRequest> boolean isNoisyRequest(R request) {
        // Consumers send these packets very often
        return (request instanceof HeartbeatRequest)
                || (request instanceof OffsetCommitRequest
                || (request instanceof EndTxnRequest
                || (request instanceof AddPartitionsToTxnRequest)));
    }

    @Override
    protected void handleInitProducerId(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                        CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture,
                FindCoordinatorRequest.CoordinatorType.TRANSACTION,
                InitProducerIdRequest.class,
                InitProducerIdRequestData.class,
                (InitProducerIdRequestData r) -> {
                    String id = r.transactionalId();
                    // this id is used only for routing, and it must be non null here
                    return id != null ? id : UUID.randomUUID().toString();
                },
                null);
    }

    @Override
    protected void handleAddPartitionsToTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                            CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture,
                FindCoordinatorRequest.CoordinatorType.TRANSACTION,
                AddPartitionsToTxnRequest.class,
                AddPartitionsToTxnRequestData.class,
                AddPartitionsToTxnRequestData::transactionalId,
                null);
    }

    @Override
    protected void handleAddOffsetsToTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                         CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture,
                FindCoordinatorRequest.CoordinatorType.TRANSACTION,
                AddOffsetsToTxnRequest.class,
                AddOffsetsToTxnRequestData.class,
                AddOffsetsToTxnRequestData::transactionalId,
                null);
    }

    @Override
    protected void handleTxnOffsetCommit(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                         CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                TxnOffsetCommitRequest.class,
                TxnOffsetCommitRequestData.class,
                TxnOffsetCommitRequestData::groupId,
                null);
    }

    @Override
    protected void handleEndTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture,
                FindCoordinatorRequest.CoordinatorType.TRANSACTION,
                EndTxnRequest.class,
                EndTxnRequestData.class,
                EndTxnRequestData::transactionalId,
                null);
    }

    @Override
    protected void handleWriteTxnMarkers(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                         CompletableFuture<AbstractResponse> resultFuture) {
        resultFuture.completeExceptionally(new UnsupportedOperationException("not a proxy operation"));
    }

    private ConnectionToBroker grabConnectionToBroker(String brokerHost, int brokerPort) {
        String connectionKey = brokerHost + ":" + brokerPort;
        return connectionsToBrokers.computeIfAbsent(connectionKey, (brokerAddress) -> {
            return new ConnectionToBroker(this, connectionKey, brokerHost, brokerPort);
        });
    }

    void discardConnectionToBroker(ConnectionToBroker connectionToBroker) {
        connectionsToBrokers.compute(connectionToBroker.connectionKey, (key, existing) -> {
            if (existing == connectionToBroker) {
                // only remove if it is actually the same object
                return null;
            } else {
                return existing;
            }
        });
    }

    void forgetMetadataForFailedBroker(String brokerHost, int brokerPort) {
        Collection<String> keysToRemove = topicsLeaders
                .entrySet()
                .stream()
                .filter(n -> n.getValue().port() == brokerPort && n.getValue().host().equals(brokerHost))
                .map(e -> e.getKey())
                .collect(Collectors.toSet());
        log.info("forgetMetadataForFailedBroker {}:{} -> {}", brokerHost, brokerPort, keysToRemove);
        keysToRemove.forEach(topicsLeaders::remove);
    }

    String getClientToken() throws PulsarClientException {
        return authenticationToken.getAuthData().getCommandData();
    }

    /**
     * If we are using kafkaEnableMultiTenantMetadata we need to ensure
     * that the TenantSpec refer to an existing tenant.
     * @param session
     * @return whether the tenant is accessible
     */
    private boolean validateTenantAccessForSession(Session session)
            throws AuthenticationException {
        if (!kafkaConfig.isKafkaEnableMultiTenantMetadata()) {
            // we are not leveraging kafkaEnableMultiTenantMetadata feature
            // the client will access only system tenant
            return true;
        }
        String tenantSpec = session.getPrincipal().getTenantSpec();
        if (tenantSpec == null) {
            // we are not leveraging kafkaEnableMultiTenantMetadata feature
            // the client will access only system tenant
            return true;
        }
        String currentTenant = extractTenantFromTenantSpec(tenantSpec);
        try {
            Boolean granted = authorize(AclOperation.ANY,
                    Resource.of(ResourceType.TENANT, currentTenant), session)
                    .get();
            return granted != null && granted;
        } catch (ExecutionException | InterruptedException err) {
            if (err.getCause() != null
                    && (err.getCause() instanceof PulsarAdminException.NotAuthorizedException
                    || err.getCause().getCause() instanceof PulsarAdminException.NotAuthorizedException)) {
                log.info("Error while verifying tenant access for {}: {}", currentTenant, err + "");
                return false;
            }
            log.error("Internal error while verifying tenant access for {}", currentTenant, err);
            throw new AuthenticationException("Internal error while verifying tenant access for "
                    + currentTenant + " :" + err, err);
        }
    }
}
