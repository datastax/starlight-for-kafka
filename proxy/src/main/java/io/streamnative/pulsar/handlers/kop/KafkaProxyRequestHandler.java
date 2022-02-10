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

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.ApiKeys;
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
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
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
    private final ConcurrentHashMap<String, Node> topicsLeaders = new ConcurrentHashMap<>();
    private final Function<String, String> brokerAddressMapper;
    private final EventLoopGroup workerGroup;
    private final ConcurrentHashMap<String, ConnectionToBroker> connectionsToBrokers = new ConcurrentHashMap<>();
    private AtomicInteger dummyCorrelationIdGenerator = new AtomicInteger(-1);
    private volatile boolean coordinatorNamespaceExists = false;

    public KafkaProxyRequestHandler(String id, KafkaProtocolProxyMain.PulsarAdminProvider pulsarAdmin,
                                    AuthenticationService authenticationService,
                                    KafkaServiceConfiguration kafkaConfig,
                                    boolean tlsEnabled,
                                    EndPoint advertisedEndPoint,
                                    Function<String, String> brokerAddressMapper,
                                    EventLoopGroup workerGroup,
                                    RequestStats requestStats) throws Exception {
        super(requestStats, kafkaConfig, null);
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
        }, kafkaConfig))
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

    static PartitionMetadata newPartitionMetadata(TopicName topicName, Node node) {
        int pulsarPartitionIndex = topicName.getPartitionIndex();
        int kafkaPartitionIndex = pulsarPartitionIndex == -1 ? 0 : pulsarPartitionIndex;
        return new PartitionMetadata(
                Errors.NONE,
                kafkaPartitionIndex,
                node,                      // leader
                Optional.empty(),          // leaderEpoch is unknown in Pulsar
                Lists.newArrayList(node),  // replicas
                Lists.newArrayList(node),  // isr
                Collections.emptyList()     // offline replicas
        );
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
            authenticator.sendAuthenticationFailureResponse();
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
        List<ApiVersionsResponse.ApiVersion> versionList = new ArrayList<>();
        if (unsupportedApiVersion) {
            return new ApiVersionsResponse(0, Errors.UNSUPPORTED_VERSION, versionList);
        } else {
            for (ApiKeys apiKey : ApiKeys.values()) {
                if (apiKey.minRequiredInterBrokerMagic <= RecordBatch.CURRENT_MAGIC_VALUE) {
                    switch (apiKey) {
                        case FETCH:
                            // V4 added MessageSets responses. We need to make sure RecordBatch format is not used
                            versionList.add(new ApiVersionsResponse.ApiVersion((short) 1, (short) 4,
                                    apiKey.latestVersion()));
                            break;
                        case LIST_OFFSETS:
                            // V0 is needed for librdkafka
                            versionList.add(new ApiVersionsResponse.ApiVersion((short) 2, (short) 0,
                                    apiKey.latestVersion()));
                            break;
                        default:
                            versionList.add(new ApiVersionsResponse.ApiVersion(apiKey));
                    }
                }
            }
            return new ApiVersionsResponse(0, Errors.NONE, versionList);
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
                MetadataRequest.class, (metadataRequest) -> "system",
                null);

        String namespacePrefix = currentNamespacePrefix();
        responseInterceptor.whenComplete((metadataResponse, error) -> {
            if (error != null) {
                resultFuture.completeExceptionally(error);
            } else {
                MetadataResponse responseFromBroker = (MetadataResponse) metadataResponse;
                Node selfNode = newSelfNode();
                List<Node> nodeList = Collections.singletonList(selfNode);
                MetadataResponse response = new MetadataResponse(
                        responseFromBroker.throttleTimeMs(),
                        nodeList,
                        responseFromBroker.clusterId(),
                        selfNode.id(),
                        responseFromBroker.topicMetadata()
                                .stream()
                                .map((MetadataResponse.TopicMetadata md) -> {
                                    return new MetadataResponse.TopicMetadata(
                                            md.error(),
                                            md.topic(),
                                            md.isInternal(),
                                            md
                                                    .partitionMetadata()
                                                    .stream()
                                                    .map((MetadataResponse.PartitionMetadata pd) -> {
                                                        // please note that usually the Kafka client
                                                        // opens two different connections
                                                        // for metadata and for data
                                                        // so caching this value here
                                                        // won't help to serve Produce or Fetch requests
                                                        String fullTopicName = KopTopic.toString(md.topic(),
                                                                pd.partition(), namespacePrefix);
                                                        topicsLeaders.put(fullTopicName, pd.leader());
                                                        return new MetadataResponse.PartitionMetadata(pd.error(),
                                                                pd.partition(),
                                                                selfNode, Optional.empty(), nodeList, nodeList,
                                                                Collections.<Node>emptyList());
                                                    })
                                                    .collect(Collectors.toList()));
                                })
                                .collect(Collectors.toList())
                );
                resultFuture.complete(response);
            }
        });
    }

    protected void handleProduceRequest(KafkaHeaderAndRequest produceHar,
                                        CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(produceHar.getRequest() instanceof ProduceRequest);
        ProduceRequest produceRequest = (ProduceRequest) produceHar.getRequest();
        final int numPartitions = produceRequest.partitionRecordsOrFail().size();
        if (numPartitions == 0) {
            resultFuture.complete(new ProduceResponse(new HashMap<>()));
            return;
        }

        final Map<TopicPartition, PartitionResponse> responseMap = new ConcurrentHashMap<>();
        // delay produce
        final AtomicInteger topicPartitionNum = new AtomicInteger(produceRequest.partitionRecordsOrFail().size());

        String namespacePrefix = currentNamespacePrefix();
        final String metadataNamespace = kafkaConfig.getKafkaMetadataNamespace();
        // validate system topics
        for (TopicPartition topicPartition : produceRequest.partitionRecordsOrFail().keySet()) {
            final String fullPartitionName = KopTopic.toString(topicPartition, namespacePrefix);
            // check KOP inner topic
            if (KopTopic.isInternalTopic(metadataNamespace, fullPartitionName)) {
                log.error("[{}] Request {}: not support produce message to inner topic. topic: {}",
                        ctx.channel(), produceHar.getHeader(), topicPartition);
                Map<TopicPartition, PartitionResponse> errorsMap =
                        produceRequest.partitionRecordsOrFail()
                                .keySet()
                                .stream()
                                .collect(Collectors.toMap(Function.identity(),
                                        p -> new PartitionResponse(Errors.INVALID_TOPIC_EXCEPTION)));
                resultFuture.complete(new ProduceResponse(errorsMap));
                return;
            }
        }

        Map<String, PartitionMetadata> brokers = new ConcurrentHashMap<>();
        List<CompletableFuture<?>> lookups = new ArrayList<>(topicPartitionNum.get());
        produceRequest.partitionRecordsOrFail().forEach((topicPartition, records) -> {
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
                    produceRequest.partitionRecordsOrFail()
                            .keySet()
                            .stream()
                            .collect(Collectors.toMap(Function.identity(),
                                    p -> new PartitionResponse(Errors.REQUEST_TIMED_OUT)));
            resultFuture.complete(new ProduceResponse(errorsMap));
            return;
        }

        boolean multipleBrokers = false;
        // check if all the partitions are for the same broker
        PartitionMetadata first = null;
        for (PartitionMetadata md : brokers.values()) {
            if (first == null) {
                first = md;
            } else if (!first.leader().equals(md.leader())) {
                multipleBrokers = true;
                break;
            }
        }


        if (!multipleBrokers) {
            // all the partitions are owned by one single broker,
            // we can forward the whole request to the only broker
            final PartitionMetadata broker = first;
            log.debug("forward FULL produce id {} of {} parts to {}", produceHar.getHeader().correlationId(),
                    numPartitions, broker);
            grabConnectionToBroker(broker.leader().host(), broker.leader().port()).
                    forwardRequest(produceHar)
                    .thenAccept(response -> {
                        ProduceResponse resp = (ProduceResponse) response;
                        resp.responses().forEach((topicPartition, topicResp) -> {
                            invalidateLeaderIfNeeded(namespacePrefix, broker.leader(), topicPartition, topicResp.error);
                            if (topicResp.error == Errors.NONE) {
                                log.debug("forward FULL produce id {} COMPLETE  of {} parts to {}",
                                        produceHar.getHeader().correlationId(), numPartitions, broker);
                            }
                        });
                        resultFuture.complete(response);
                    }).exceptionally(badError -> {
                        log.error("Full Produce failed", badError);
                        // REQUEST_TIMED_OUT triggers a new trials on the client
                        Map<TopicPartition, PartitionResponse> errorsMap =
                                produceRequest.partitionRecordsOrFail()
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
                produceRequest.partitionRecordsOrFail().keySet().forEach(topicPartition -> {
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
            final Map<Node, ProduceRequest> requestsPerBroker = new HashMap<>();
            produceRequest.partitionRecordsOrFail().forEach((topicPartition, records) -> {
                final String fullPartitionName = KopTopic.toString(topicPartition, namespacePrefix);
                PartitionMetadata topicMetadata = brokers.get(fullPartitionName);
                Node kopBroker = topicMetadata.leader();

                ProduceRequest produceRequestPerBroker = requestsPerBroker.computeIfAbsent(kopBroker, a -> {
                    return ProduceRequest.Builder.forCurrentMagic((short) 1,
                                    produceRequest.timeout(),
                                    new HashMap<>())
                            .build();
                });
                produceRequestPerBroker.partitionRecordsOrFail().put(topicPartition, records);
            });
            requestsPerBroker.forEach((kopBroker, requestForSinglePartition) -> {
                int dummyCorrelationId = getDummyCorrelationId();

                RequestHeader header = new RequestHeader(
                        produceHar.getHeader().apiKey(),
                        produceHar.getHeader().apiVersion(),
                        produceHar.getHeader().clientId(),
                        dummyCorrelationId
                );

                ByteBuffer buffer = requestForSinglePartition.serialize(header);

                KafkaHeaderAndRequest singlePartitionRequest = new KafkaHeaderAndRequest(
                        header,
                        requestForSinglePartition,
                        Unpooled.wrappedBuffer(buffer),
                        null
                );

                if (log.isDebugEnabled()) {
                    log.debug("forward produce for {} to {}",
                            requestForSinglePartition.partitionRecordsOrFail().keySet(), kopBroker);
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
                                    invalidateLeaderIfNeeded(namespacePrefix, kopBroker, topicPartition, partitionResponse.error);
                                    addPartitionResponse.accept(topicPartition, partitionResponse);
                                }
                            });
                        }).exceptionally(badError -> {
                            log.error("bad error during split produce for {}",
                                    requestForSinglePartition.partitionRecordsOrFail().keySet(), badError);
                            requestForSinglePartition.partitionRecordsOrFail().keySet().forEach(topicPartition
                                    -> addPartitionResponse.accept(topicPartition,
                                    new PartitionResponse(Errors.REQUEST_TIMED_OUT)));
                            return null;
                        });
            });
        }


    }

    private void invalidateLeaderIfNeeded(String namespacePrefix, Node kopBroker, TopicPartition topicPartition,
                                          Errors error) {
        if (error == Errors.NOT_LEADER_FOR_PARTITION) {
            log.info("Broker {} is no more the leader for {} (topicsLeaders {})", kopBroker, topicPartition, topicsLeaders);
            final String fullPartitionName = KopTopic.toString(topicPartition, namespacePrefix);
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

        final int numPartitions = fetchRequest.fetchData().size();
        if (numPartitions == 0) {
            resultFuture.complete(new FetchResponse(Errors.NONE, new LinkedHashMap<>(), 0,
                    fetchRequest.metadata().sessionId()));
            return;
        }

        String namespacePrefix = currentNamespacePrefix();
        Map<TopicPartition, FetchResponse.PartitionData<?>> responseMap = new ConcurrentHashMap<>();
        final AtomicInteger topicPartitionNum = new AtomicInteger(fetchRequest.fetchData().size());
        final String metadataNamespace = kafkaConfig.getKafkaMetadataNamespace();

        // validate system topics
        for (TopicPartition topicPartition : fetchRequest.fetchData().keySet()) {
            final String fullPartitionName = KopTopic.toString(topicPartition, namespacePrefix);
            // check KOP inner topic
            if (KopTopic.isInternalTopic(metadataNamespace, fullPartitionName)) {
                log.error("[{}] Request {}: not support fetch message to inner topic. topic: {}",
                        ctx.channel(), fetch.getHeader(), topicPartition);
                Map<TopicPartition, FetchResponse.PartitionData<?>> errorsMap =
                        fetchRequest.fetchData()
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

        Map<String, PartitionMetadata> brokers = new ConcurrentHashMap<>();
        List<CompletableFuture<?>> lookups = new ArrayList<>(topicPartitionNum.get());
        fetchRequest.fetchData().forEach((topicPartition, partitionData) -> {
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
                                fetchRequest.fetchData()
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
                        PartitionMetadata first = null;
                        for (PartitionMetadata md : brokers.values()) {
                            if (first == null) {
                                first = md;
                            } else if (!first.leader().equals(md.leader())) {
                                multipleBrokers = true;
                                break;
                            }
                        }


                        if (!multipleBrokers) {
                            // all the partitions are owned by one single broker,
                            // we can forward the whole request to the only broker
                            log.debug("forward FULL fetch of {} parts to {}", numPartitions, first);
                            grabConnectionToBroker(first.leader().host(), first.leader().port()).
                                    forwardRequest(fetch)
                                    .thenAccept(response -> {
                                        resultFuture.complete(response);
                                    }).exceptionally(badError -> {
                                        log.error("bad error for FULL fetch", badError);
                                        Map<TopicPartition, FetchResponse.PartitionData<?>> errorsMap =
                                                fetchRequest.fetchData()
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
                                fetchRequest.fetchData().keySet().forEach(topicPartition -> {
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

                            Map<Node, FetchRequest> requestsByBroker = new HashMap<>();

                            fetchRequest.fetchData().forEach((topicPartition, partitionData) -> {
                                final String fullPartitionName = KopTopic.toString(topicPartition, namespacePrefix);
                                PartitionMetadata topicMetadata = brokers.get(fullPartitionName);
                                Node kopBroker = topicMetadata.leader();
                                FetchRequest requestForSinglePartition =
                                        requestsByBroker.computeIfAbsent(kopBroker, ___ -> FetchRequest.Builder
                                                .forConsumer(((FetchRequest) fetch.getRequest()).maxWait(),
                                                        ((FetchRequest) fetch.getRequest()).minBytes(),
                                                        new HashMap<>())
                                                .build());

                                requestForSinglePartition.fetchData().put(topicPartition, partitionData);
                            });

                            requestsByBroker.forEach((kopBroker, requestForSingleBroker) -> {
                                int dummyCorrelationId = getDummyCorrelationId();
                                RequestHeader header = new RequestHeader(
                                        fetch.getHeader().apiKey(),
                                        fetch.getHeader().apiVersion(),
                                        fetch.getHeader().clientId(),
                                        dummyCorrelationId
                                );
                                ByteBuffer buffer = requestForSingleBroker.serialize(header);
                                KafkaHeaderAndRequest singlePartitionRequest = new KafkaHeaderAndRequest(
                                        header,
                                        requestForSingleBroker,
                                        Unpooled.wrappedBuffer(buffer),
                                        null
                                );

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
                                                        invalidateLeaderIfNeeded(namespacePrefix, kopBroker, topicPartition, partitionResponse.error);
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
                                        });
                            });
                        }
                    }
                });
    }

    private CompletableFuture<PartitionMetadata> findCoordinator(FindCoordinatorRequest.CoordinatorType type,
                                                                 String key) {
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
        AbstractResponse response = new FindCoordinatorResponse(
                Errors.NONE,
                newSelfNode());
        resultFuture.complete(response);

    }

    protected void handleJoinGroupRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                JoinGroupRequest.class, JoinGroupRequest::groupId,
                null);
    }

    protected void handleSyncGroupRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                SyncGroupRequest.class, SyncGroupRequest::groupId,
                null);
    }

    protected void handleHeartbeatRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                HeartbeatRequest.class, HeartbeatRequest::groupId,
                null);
    }

    @Override
    protected void handleLeaveGroupRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                           CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                LeaveGroupRequest.class, LeaveGroupRequest::groupId,
                null);
    }

    @Override
    protected void handleDescribeGroupRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                              CompletableFuture<AbstractResponse> resultFuture) {
        // forward to the coordinator of the first group
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                DescribeGroupsRequest.class,
                (DescribeGroupsRequest r) -> r.groupIds().get(0),
                null);
    }

    @Override
    protected void handleListGroupsRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                           CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                ListGroupsRequest.class,
                (metadataRequest) -> "system",
                null);
    }

    @Override
    protected void handleDeleteGroupsRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                             CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                DeleteGroupsRequest.class,
                (metadataRequest) -> "system",
                null);
    }

    @Override
    protected void handleSaslAuthenticate(KafkaHeaderAndRequest saslAuthenticate,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        resultFuture.complete(new SaslAuthenticateResponse(Errors.ILLEGAL_SASL_STATE,
                "SaslAuthenticate request received after successful authentication"));
    }

    @Override
    protected void handleSaslHandshake(KafkaHeaderAndRequest saslHandshake,
                                       CompletableFuture<AbstractResponse> resultFuture) {
        resultFuture.complete(new SaslHandshakeResponse(Errors.ILLEGAL_SASL_STATE, Collections.emptySet()));
    }

    @Override
    protected void handleCreateTopics(KafkaHeaderAndRequest createTopics,
                                      CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(createTopics, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                CreateTopicsRequest.class, (metadataRequest) -> "system",
                null);
    }

    @Override
    protected void handleCreatePartitions(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                CreatePartitionsRequest.class, (metadataRequest) -> "system",
                null);
    }

    protected void handleDescribeConfigs(KafkaHeaderAndRequest describeConfigs,
                                         CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(describeConfigs, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                DescribeConfigsRequest.class, (metadataRequest) -> "system",
                null);
    }

    protected void handleAlterConfigs(KafkaHeaderAndRequest describeConfigs,
                                      CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(describeConfigs, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                AlterConfigsRequest.class, (metadataRequest) -> "system",
                null);
    }

    @Override
    protected void handleDeleteTopics(KafkaHeaderAndRequest deleteTopics,
                                      CompletableFuture<AbstractResponse> resultFuture) {

        DeleteTopicsRequest request = (DeleteTopicsRequest) deleteTopics.getRequest();
        if (request.topics().isEmpty()) {
            resultFuture.complete(new DeleteTopicsResponse(new HashMap<>()));
            return;
        }
        String namespacePrefix = currentNamespacePrefix();
        Set<String> topics = request.topics();
        Map<String, Errors> responseMap = new ConcurrentHashMap<>();
        int numTopics = topics.size();
        Map<String, PartitionMetadata> brokers = new ConcurrentHashMap<>();
        List<CompletableFuture<?>> lookups = new ArrayList<>(numTopics);
        topics.forEach((topic) -> {
            KopTopic kopTopic;
            try {
                kopTopic = new KopTopic(topic, namespacePrefix);
            } catch (KoPTopicException var6) {
                responseMap.put(topic, Errors.UNKNOWN_TOPIC_OR_PARTITION);
                return;
            }
            final String fullPartitionName = kopTopic.getFullName();
            lookups.add(findBroker(TopicName.get(fullPartitionName))
                    .thenAccept(p -> brokers.put(fullPartitionName, p)));
        });

        handleRequestWithCoordinator(deleteTopics, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                DeleteTopicsRequest.class, (metadataRequest) -> "system",
                null);
    }

    @Override
    protected void handleDeleteRecords(KafkaHeaderAndRequest deleteRecords,
                                       CompletableFuture<AbstractResponse> resultFuture) {

        DeleteRecordsRequest request = (DeleteRecordsRequest) deleteRecords.getRequest();
        Map<TopicPartition, Long> partitionOffsets = request.partitionOffsets();
        if (partitionOffsets.isEmpty()) {
            resultFuture.complete(KafkaResponseUtils.newDeleteRecords(Collections.emptyMap()));
            return;
        }
        String namespacePrefix = currentNamespacePrefix();

        Map<TopicPartition, DeleteRecordsResponse.PartitionResponse> responseMap = new ConcurrentHashMap<>();
        int numPartitions = partitionOffsets.size();
        Map<String, PartitionMetadata> brokers = new ConcurrentHashMap<>();
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
                        Map<TopicPartition, DeleteRecordsResponse.PartitionResponse> errorsMap =
                                partitionOffsets
                                        .keySet()
                                        .stream()
                                        .collect(Collectors.toMap(Function.identity(),
                                                p -> new DeleteRecordsResponse.PartitionResponse(0,
                                                        Errors.UNKNOWN_SERVER_ERROR)));
                        resultFuture.complete(KafkaResponseUtils.newDeleteRecords(errorsMap));
                    } else {
                        boolean multipleBrokers = false;

                        // check if all the partitions are for the same broker
                        PartitionMetadata first = null;
                        for (PartitionMetadata md : brokers.values()) {
                            if (first == null) {
                                first = md;
                            } else if (!first.leader().equals(md.leader())) {
                                multipleBrokers = true;
                                break;
                            }
                        }


                        if (!multipleBrokers) {
                            // all the partitions are owned by one single broker,
                            // we can forward the whole request to the only broker
                            log.debug("forward FULL DeleteRecords of {} parts to {}", numPartitions, first);
                            grabConnectionToBroker(first.leader().host(), first.leader().port()).
                                    forwardRequest(deleteRecords)
                                    .thenAccept(response -> {
                                        resultFuture.complete(response);
                                    }).exceptionally(badError -> {
                                        log.error("bad error for FULL DeleteRecords", badError);
                                        Map<TopicPartition, DeleteRecordsResponse.PartitionResponse> errorsMap =
                                                partitionOffsets
                                                        .keySet()
                                                        .stream()
                                                        .collect(Collectors.toMap(Function.identity(),
                                                                p -> new DeleteRecordsResponse.PartitionResponse(0,
                                                                        Errors.UNKNOWN_SERVER_ERROR)));
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
                                        responseMap.put(topicPartition,
                                                new DeleteRecordsResponse.PartitionResponse(0,
                                                        Errors.UNKNOWN_SERVER_ERROR));
                                    }
                                });
                                if (log.isDebugEnabled()) {
                                    log.debug("[{}] Request {}: Complete handle DeleteRecords.", ctx.channel(),
                                            deleteRecords);
                                }
                                final LinkedHashMap<TopicPartition, DeleteRecordsResponse.PartitionResponse>
                                        responseMapRaw =
                                        new LinkedHashMap<>(responseMap);
                                resultFuture.complete(KafkaResponseUtils.newDeleteRecords(responseMapRaw));
                            };
                            BiConsumer<TopicPartition, DeleteRecordsResponse.PartitionResponse>
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

                            final BiConsumer<TopicPartition, DeleteRecordsResponse.PartitionResponse> resultConsumer =
                                    (topicPartition, data) -> addDeletePartitionResponse.accept(
                                            topicPartition, data);
                            final BiConsumer<TopicPartition, Errors> errorsConsumer =
                                    (topicPartition, errors) -> addDeletePartitionResponse.accept(topicPartition,
                                            new DeleteRecordsResponse.PartitionResponse(0, errors));

                            Map<Node, DeleteRecordsRequest> requestsByBroker = new HashMap<>();

                            partitionOffsets.forEach((topicPartition, offset) -> {
                                final String fullPartitionName = KopTopic.toString(topicPartition, namespacePrefix);
                                PartitionMetadata topicMetadata = brokers.get(fullPartitionName);
                                Node kopBroker = topicMetadata.leader();
                                DeleteRecordsRequest requestForSinglePartition = requestsByBroker
                                        .computeIfAbsent(kopBroker, ___ -> new DeleteRecordsRequest
                                                .Builder(request.timeout(), new HashMap<>()).build());

                                requestForSinglePartition.partitionOffsets().put(topicPartition, offset);
                            });

                            requestsByBroker.forEach((kopBroker, requestForSingleBroker) -> {
                                int dummyCorrelationId = getDummyCorrelationId();
                                RequestHeader header = new RequestHeader(
                                        deleteRecords.getHeader().apiKey(),
                                        deleteRecords.getHeader().apiVersion(),
                                        deleteRecords.getHeader().clientId(),
                                        dummyCorrelationId
                                );
                                ByteBuffer buffer = requestForSingleBroker.serialize(header);
                                KafkaHeaderAndRequest singlePartitionRequest = new KafkaHeaderAndRequest(
                                        header,
                                        requestForSingleBroker,
                                        Unpooled.wrappedBuffer(buffer),
                                        null
                                );

                                if (log.isDebugEnabled()) {
                                    log.debug("forward DeleteRequest for {} to {}",
                                            requestForSingleBroker.partitionOffsets().keySet(), kopBroker);
                                }
                                grabConnectionToBroker(kopBroker.host(), kopBroker.port())
                                        .forwardRequest(singlePartitionRequest)
                                        .thenAccept(response -> {
                                            DeleteRecordsResponse resp = (DeleteRecordsResponse) response;
                                            resp.responses()
                                                    .forEach((topicPartition, partitionResponse) -> {
                                                        invalidateLeaderIfNeeded(namespacePrefix, kopBroker, topicPartition, partitionResponse.error);
                                                        if (log.isDebugEnabled()) {
                                                            final String fullPartitionName =
                                                                    KopTopic.toString(topicPartition,
                                                                            namespacePrefix);
                                                            log.debug("result fetch for {} to {} {}", fullPartitionName,
                                                                    kopBroker,
                                                                    partitionResponse);
                                                        }
                                                        addDeletePartitionResponse.accept(topicPartition,
                                                                partitionResponse);
                                                    });
                                        }).exceptionally(badError -> {
                                            log.error("bad error while fetching for {} from {}",
                                                    requestForSingleBroker.partitionOffsets().keySet(), badError,
                                                    kopBroker);
                                            requestForSingleBroker.partitionOffsets().keySet().forEach(topicPartition ->
                                                    errorsConsumer.accept(topicPartition, Errors.UNKNOWN_SERVER_ERROR)
                                            );
                                            return null;
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
        } catch (PulsarClientException err) {
            return FutureUtil.failedFuture(err);
        }
    }

    public CompletableFuture<PartitionMetadata> findBroker(TopicName topic) {
        return findBroker(topic, false);
    }

    public CompletableFuture<PartitionMetadata> findBroker(TopicName topic, boolean system) {
        if (log.isTraceEnabled()) {
            log.trace("[{}] Handle Lookup for {}", ctx.channel(), topic);
        }
        CompletableFuture<PartitionMetadata> returnFuture = new CompletableFuture<>();

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
                    final EndPoint endPoint = EndPoint.getPlainTextEndPoint(listeners);
                    final Node node = newNode(endPoint.getInetAddress());

                    if (log.isTraceEnabled()) {
                        log.trace("Found broker localListeners: {} for topicName: {}, "
                                        + "localListeners: {}, found Listeners: {}",
                                listeners, topic, advertisedListeners, listeners);
                    }

                    topicsLeaders.put(topic.toString(), node);
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
        log.error("{} ListOffset v0 is not supported", this);
        resultFuture.complete(listOffset
                .getRequest()
                .getErrorResponse(new Exception("V0 not supported")));
    }

    protected void handleListOffsetRequestV1(KafkaHeaderAndRequest listOffset,
                                             CompletableFuture<AbstractResponse> resultFuture) {
        // use partitionTimestamps
        checkArgument(listOffset.getRequest() instanceof ListOffsetRequest);
        ListOffsetRequest request = (ListOffsetRequest) listOffset.getRequest();

        Map<TopicPartition, ListOffsetResponse.PartitionData> map = new ConcurrentHashMap<>();
        AtomicInteger expectedCount = new AtomicInteger(request.partitionTimestamps().size());

        BiConsumer<String, ListOffsetResponse> onResponse = (topic, topicResponse) -> {
            topicResponse.responseData().forEach((tp, data) -> {
                map.put(tp, data);
                if (expectedCount.decrementAndGet() == 0) {
                    ListOffsetResponse response = new ListOffsetResponse(map);
                    resultFuture.complete(response);
                }
            });
        };

        if (request.partitionTimestamps().isEmpty()) {
            // this should not happen
            ListOffsetResponse response = new ListOffsetResponse(map);
            resultFuture.complete(response);
            return;
        }
        String namespacePrefix = currentNamespacePrefix();
        for (Map.Entry<TopicPartition, ListOffsetRequest.PartitionData> entry : request.partitionTimestamps()
                .entrySet()) {
            final String fullPartitionName = KopTopic.toString(entry.getKey(), namespacePrefix);

            int dummyCorrelationId = getDummyCorrelationId();
            RequestHeader header = new RequestHeader(
                    listOffset.getHeader().apiKey(),
                    listOffset.getHeader().apiVersion(),
                    listOffset.getHeader().clientId(),
                    dummyCorrelationId
            );

            Map<TopicPartition, ListOffsetRequest.PartitionData> tsData = new HashMap<>();
            tsData.put(entry.getKey(), entry.getValue());
            ListOffsetRequest requestForSinglePartition = ListOffsetRequest.Builder
                    .forConsumer(false, request.isolationLevel())
                    .setTargetTimes(tsData)
                    .build(request.version());
            ByteBuffer buffer = requestForSinglePartition.serialize(header);

            KafkaHeaderAndRequest singlePartitionRequest = new KafkaHeaderAndRequest(
                    header,
                    requestForSinglePartition,
                    Unpooled.wrappedBuffer(buffer),
                    null
            );

            findBroker(TopicName.get(fullPartitionName))
                    .thenAccept(brokerAddress -> {
                        grabConnectionToBroker(brokerAddress.leader().host(), brokerAddress.leader().port())
                                .forwardRequest(singlePartitionRequest)
                                .thenAccept(theResponse -> {
                                    onResponse.accept(fullPartitionName, (ListOffsetResponse) theResponse);
                                }).exceptionally(err -> {
                                    ListOffsetResponse dummyResponse = new ListOffsetResponse(new HashMap<>());
                                    dummyResponse.responseData().put(entry.getKey(),
                                            new ListOffsetResponse.PartitionData(Errors.BROKER_NOT_AVAILABLE,
                                                    0, 0, Optional.empty()));
                                    onResponse.accept(fullPartitionName, dummyResponse);
                                    return null;
                                });
                    }).exceptionally(err -> {
                        ListOffsetResponse dummyResponse = new ListOffsetResponse(new HashMap<>());
                        dummyResponse.responseData().put(entry.getKey(),
                                new ListOffsetResponse.PartitionData(Errors.BROKER_NOT_AVAILABLE,
                                        0, 0, Optional.empty()));
                        onResponse.accept(fullPartitionName, dummyResponse);
                        return null;
                    });
        }
    }

    @Override
    protected void handleOffsetFetchRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                            CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                OffsetFetchRequest.class, OffsetFetchRequest::groupId,
                null);
    }

    @Override
    protected void handleOffsetCommitRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                             CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture,
                FindCoordinatorRequest.CoordinatorType.GROUP, OffsetCommitRequest.class, OffsetCommitRequest::groupId,
                null);
    }

    private <K extends AbstractRequest, R extends AbstractResponse> void handleRequestWithCoordinator(
            KafkaHeaderAndRequest kafkaHeaderAndRequest,
            CompletableFuture<AbstractResponse> resultFuture,
            FindCoordinatorRequest.CoordinatorType coordinatorType,
            Class<K> requestClass,
            Function<K, String> keyExtractor,
            BiFunction<K, Throwable, R> customErrorBuilder
    ) {
        BiFunction<K, Throwable, R> errorBuilder;
        if (customErrorBuilder == null) {
            errorBuilder = (K request, Throwable t) -> (R) request.getErrorResponse(t);
        } else {
            errorBuilder = customErrorBuilder;
        }
        try {
            checkArgument(requestClass.isInstance(kafkaHeaderAndRequest.getRequest()));
            K request = (K) kafkaHeaderAndRequest.getRequest();

            String transactionalId = keyExtractor.apply(request);
            if (!isNoisyRequest(request)) {
                log.info("handleRequestWithCoordinator {} {} {} {}", request.getClass().getSimpleName(), request,
                        transactionalId);
            }

            findCoordinator(coordinatorType, transactionalId)
                    .thenAccept(metadata -> {
                        grabConnectionToBroker(metadata.leader().host(), metadata.leader().port())
                                .forwardRequest(kafkaHeaderAndRequest)
                                .thenAccept(serverResponse -> {
                                    if (!isNoisyRequest(request)) {
                                        log.info("Sending {} {} from {}:{} errors {}.", serverResponse,
                                                serverResponse.getClass(),
                                                metadata.leader().host(), metadata.leader().port(),
                                                serverResponse.errorCounts());
                                    }
                                    if (serverResponse.errorCounts() != null) {
                                        for (Errors error : serverResponse.errorCounts().keySet()) {
                                            if (error == Errors.NOT_COORDINATOR
                                                    || error == Errors.NOT_CONTROLLER
                                                    || error == Errors.NOT_LEADER_FOR_PARTITION) {
                                                forgetMetadataForFailedBroker(metadata.leader().host(),
                                                        metadata.leader().port());
                                            }
                                        }
                                    }
                                    resultFuture.complete(serverResponse);
                                }).exceptionally(err -> {
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
                || (request instanceof OffsetCommitRequest);
    }

    @Override
    protected void handleInitProducerId(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                        CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture,
                FindCoordinatorRequest.CoordinatorType.TRANSACTION, InitProducerIdRequest.class,
                (InitProducerIdRequest r) -> {
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
                FindCoordinatorRequest.CoordinatorType.TRANSACTION, AddPartitionsToTxnRequest.class,
                AddPartitionsToTxnRequest::transactionalId,
                null);
    }

    @Override
    protected void handleAddOffsetsToTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                         CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture,
                FindCoordinatorRequest.CoordinatorType.TRANSACTION, AddOffsetsToTxnRequest.class,
                AddOffsetsToTxnRequest::transactionalId,
                null);
    }

    @Override
    protected void handleTxnOffsetCommit(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                         CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                TxnOffsetCommitRequest.class, TxnOffsetCommitRequest::consumerGroupId,
                null);
    }

    @Override
    protected void handleEndTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture,
                FindCoordinatorRequest.CoordinatorType.TRANSACTION, EndTxnRequest.class, EndTxnRequest::transactionalId,
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
            log.error("Internal error while verifying tenant access", err);
            if (err.getCause() != null
                    && (err.getCause() instanceof PulsarAdminException.NotAuthorizedException
                    || err.getCause().getCause() instanceof PulsarAdminException.NotAuthorizedException)) {
                return false;
            }
            throw new AuthenticationException("Internal error while verifying tenant access:" + err, err);
        }
    }
}
