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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.NotImplementedException;

import io.netty.channel.ChannelHandlerContext;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataManager;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionConfig;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatter;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatterFactory;
import io.streamnative.pulsar.handlers.kop.security.SaslAuthenticator;
import io.streamnative.pulsar.handlers.kop.security.Session;
import io.streamnative.pulsar.handlers.kop.security.auth.Authorizer;
import io.streamnative.pulsar.handlers.kop.security.auth.Resource;
import io.streamnative.pulsar.handlers.kop.security.auth.ResourceType;
import io.streamnative.pulsar.handlers.kop.stats.NullStatsLogger;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.MetadataUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.*;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.AuthenticationUtil;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.Murmur3_32Hash;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.common.internals.Topic.GROUP_METADATA_TOPIC_NAME;
import static org.apache.kafka.common.internals.Topic.TRANSACTION_STATE_TOPIC_NAME;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;

/**
 * This class contains all the request handling methods.
 */
@Slf4j
@Getter
public class KafkaProxyRequestHandler extends KafkaCommandDecoder {
    public static final long DEFAULT_TIMESTAMP = 0L;

    final String id;

    private final String clusterName;
    private final ScheduledExecutorService executor;
    private final KafkaProtocolProxyMain.PulsarAdminProvider admin;
    private final SaslAuthenticator authenticator;
    private final Authorizer authorizer;
    // this is for Proxy -> Broker authentication
    private final Authentication authenticationToken;

    private final boolean tlsEnabled;
    private final EndPoint advertisedEndPoint;
    private final String advertisedListeners;
    private final int defaultNumPartitions;
    private final String offsetsTopicName;
    private final String txnTopicName;
    private final Set<String> allowedNamespaces;
    private final String groupIdStoredPath;
    @Getter
    private final EntryFormatter entryFormatter;
    private final Set<String> groupIds = new HashSet<>();
    private final ConcurrentHashMap<String, Node> topicsLeaders = new ConcurrentHashMap<>();
    private final Function<String, String> brokerAddressMapper;
    private final EventLoopGroup workerGroup;

    public KafkaProxyRequestHandler(String id, KafkaProtocolProxyMain.PulsarAdminProvider pulsarAdmin,
                                    AuthenticationService authenticationService,
                                    KafkaServiceConfiguration kafkaConfig,
                                    boolean tlsEnabled,
                                    EndPoint advertisedEndPoint,
                                    Function<String, String> brokerAddressMapper,
                                    EventLoopGroup workerGroup) throws Exception {
        super(NullStatsLogger.INSTANCE, kafkaConfig);
        this.workerGroup = workerGroup;
        this.brokerAddressMapper = brokerAddressMapper;
        this.id = id;
        String auth = kafkaConfig.getBrokerClientAuthenticationPlugin();
        String authParams = kafkaConfig.getBrokerClientAuthenticationParameters();
        this.authenticationToken = AuthenticationUtil.create(auth, authParams);

        this.clusterName = kafkaConfig.getClusterName();
        this.executor = Executors.newScheduledThreadPool(4);
        this.admin = pulsarAdmin;
        final boolean authenticationEnabled = kafkaConfig.isAuthenticationEnabled();
        this.authenticator = authenticationEnabled
                ? new SaslAuthenticator(null, authenticationService,
                kafkaConfig.getSaslAllowedMechanisms(), kafkaConfig)
                : null;
        final boolean authorizationEnabled = false;
        this.authorizer = null;
        this.tlsEnabled = tlsEnabled;
        this.advertisedEndPoint = advertisedEndPoint;
        this.advertisedListeners = kafkaConfig.getKafkaAdvertisedListeners();
        this.defaultNumPartitions = kafkaConfig.getDefaultNumPartitions();
        this.offsetsTopicName = new KopTopic(String.join("/",
                kafkaConfig.getKafkaMetadataTenant(),
                kafkaConfig.getKafkaMetadataNamespace(),
                GROUP_METADATA_TOPIC_NAME)
        ).getFullName();
        this.txnTopicName = new KopTopic(String.join("/",
                kafkaConfig.getKafkaMetadataTenant(),
                kafkaConfig.getKafkaMetadataNamespace(),
                TRANSACTION_STATE_TOPIC_NAME)
        ).getFullName();
        this.allowedNamespaces = kafkaConfig.getKopAllowedNamespaces();
        this.entryFormatter = EntryFormatterFactory.create(kafkaConfig.getEntryFormat());
        this.groupIdStoredPath = kafkaConfig.getGroupIdZooKeeperPath();

    }

    @Override
    protected void channelPrepare(ChannelHandlerContext ctx, ByteBuf requestBuf,
                                  BiConsumer<Long, Throwable> registerRequestParseLatency,
                                  BiConsumer<String, Long> registerRequestLatency) throws AuthenticationException {
        if (authenticator != null) {
            authenticator.authenticate(ctx, requestBuf, registerRequestParseLatency, registerRequestLatency);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        log.info("Client connected: {}", ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        log.info("Client disconnected {}", ctx.channel());
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


    private boolean isInternalTopic(final String fullTopicName) {
        return fullTopicName.equals(offsetsTopicName) || fullTopicName.equals(txnTopicName);
    }

    // Get all topics in the configured allowed namespaces.
    //   key: the full topic name without partition suffix, e.g. persistent://public/default/my-topic
    //   value: the partitions associated with the key, e.g. for a topic with 3 partitions,
    //     persistent://public/default/my-topic-partition-0
    //     persistent://public/default/my-topic-partition-1
    //     persistent://public/default/my-topic-partition-2
    private CompletableFuture<Map<String, List<TopicName>>> getAllTopicsAsync() {
        CompletableFuture<Map<String, List<TopicName>>> topicMapFuture = new CompletableFuture<>();
        final Map<String, List<TopicName>> topicMap = new ConcurrentHashMap<>();
        final AtomicInteger pendingNamespacesCount = new AtomicInteger(allowedNamespaces.size());
        for (String namespace : allowedNamespaces) {
            // we are using getListAsync as it returns all the partitions
            getPulsarAdmin().thenCompose(admin -> admin.topics().getListAsync(namespace))
                    .whenComplete((topics, e) -> {
                        while (e instanceof CompletionException) {
                            e = e.getCause();
                        }
                        if (e != null) {
                            if (e instanceof PulsarAdminException.NotAuthorizedException) {
                                log.debug("User {} is not allowed to list topics in namespace {}",
                                        currentUser(), namespace);
                                topics = Collections.emptyList();
                            } else {
                                topicMapFuture.completeExceptionally(e);
                            }
                        }
                        if (topicMapFuture.isCompletedExceptionally()) {
                            return;
                        }
                        for (String topic : topics) {
                            final TopicName topicName = TopicName.get(topic);
                            final String key = topicName.getPartitionedTopicName();
                            topicMap.computeIfAbsent(
                                    KopTopic.removeDefaultNamespacePrefix(key),
                                    ignored -> Collections.synchronizedList(new ArrayList<>())
                            ).add(topicName);
                        }
                        if (pendingNamespacesCount.decrementAndGet() == 0) {
                            topicMapFuture.complete(topicMap);
                        }
                    });
        }
        return topicMapFuture;
    }

    protected void handleTopicMetadataRequest(KafkaHeaderAndRequest metadataHar,
                                              CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(metadataHar.getRequest() instanceof MetadataRequest);

        MetadataRequest metadataRequest = (MetadataRequest) metadataHar.getRequest();
        log.debug("handleTopicMetadataRequest for topics {} ", metadataHar.getHeader(), metadataRequest.topics());

        // Command response for all topics
        List<TopicMetadata> allTopicMetadata = Collections.synchronizedList(Lists.newArrayList());
        List<Node> allNodes = Collections.synchronizedList(Lists.newArrayList());

        List<String> topics = metadataRequest.topics();
        // topics in format : persistent://%s/%s/abc-partition-x, will be grouped by as:
        //      Entry<abc, List[TopicName]>

        // A future for a map from <kafka topic> to <pulsarPartitionTopics>:
        //      e.g. <topic1, {persistent://public/default/topic1-partition-0,...}>
        //   1. no topics provided, get all topics from namespace;
        //   2. topics provided, get provided topics.
        CompletableFuture<Map<String, List<TopicName>>> pulsarTopicsFuture;

        // Map for <partition-zero, non-partitioned-topic>, use for findBroker
        // e.g. <persistent://public/default/topic1-partition-0, persistent://public/default/topic1>
        final Map<String, TopicName> nonPartitionedTopicMap = Maps.newConcurrentMap();

        if (topics == null || topics.isEmpty()) {
            // clean all cache when get all metadata for librdkafka(<1.0.0).
            KafkaTopicManager.clearTopicManagerCache();
            // get all topics, filter by permissions.
            pulsarTopicsFuture = getAllTopicsAsync().thenApply((allTopicMap) -> {
                final Map<String, List<TopicName>> topicMap = new ConcurrentHashMap<>();
                allTopicMap.forEach((topic, list) -> {
                    list.forEach((topicName ->
                            authorize(AclOperation.DESCRIBE, Resource.of(ResourceType.TOPIC, topicName.toString()))
                                    .whenComplete((authorized, ex) -> {
                                        if (ex != null || !authorized) {
                                            allTopicMetadata.add(new TopicMetadata(
                                                    Errors.TOPIC_AUTHORIZATION_FAILED,
                                                    topic,
                                                    isInternalTopic(topicName.toString()),
                                                    Collections.emptyList()));
                                            return;
                                        }
                                        topicMap.computeIfAbsent(
                                                topic,
                                                ignored -> Collections.synchronizedList(new ArrayList<>())
                                        ).add(topicName);
                                    })));
                });

                return topicMap;
            });
        } else {
            pulsarTopicsFuture = new CompletableFuture<>();
            // get only the provided topics
            final Map<String, List<TopicName>> pulsarTopics = Maps.newConcurrentMap();

            List<String> requestTopics = metadataRequest.topics();
            final int topicsNumber = requestTopics.size();
            AtomicInteger topicsCompleted = new AtomicInteger(0);

            final Runnable completeOneTopic = () -> {
                if (topicsCompleted.incrementAndGet() == topicsNumber) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Request {}: Completed get {} topic's partitions",
                                ctx.channel(), metadataHar.getHeader(), topicsNumber);
                    }
                    pulsarTopicsFuture.complete(pulsarTopics);
                }
            };

            final BiConsumer<String, Integer> addTopicPartition = (topic, partition) -> {
                final KopTopic kopTopic = new KopTopic(topic);
                pulsarTopics.putIfAbsent(topic,
                        IntStream.range(0, partition)
                                .mapToObj(i -> TopicName.get(kopTopic.getPartitionName(i)))
                                .collect(Collectors.toList()));
                completeOneTopic.run();
            };

            final BiConsumer<String, String> completeOneAuthFailedTopic = (topic, fullTopicName) -> {
                allTopicMetadata.add(new TopicMetadata(
                        Errors.TOPIC_AUTHORIZATION_FAILED,
                        topic,
                        isInternalTopic(fullTopicName),
                        Collections.emptyList()));
                completeOneTopic.run();
            };

            requestTopics.forEach(topic -> {
                final String fullTopicName = new KopTopic(topic).getFullName();

                authorize(AclOperation.DESCRIBE, Resource.of(ResourceType.TOPIC, fullTopicName))
                        .whenComplete((authorized, ex) -> {
                            if (ex != null) {
                                log.error("Describe topic authorize failed, topic - {}. {}",
                                        fullTopicName, ex.getMessage());
                                // Authentication failed
                                completeOneAuthFailedTopic.accept(topic, fullTopicName);
                                return;
                            }
                            if (!authorized) {
                                // Permission denied
                                completeOneAuthFailedTopic.accept(topic, fullTopicName);
                                return;
                            }
                            // get partition numbers for each topic.
                            // If topic doesn't exist and allowAutoTopicCreation is enabled,
                            // the topic will be created first.
                            getPulsarAdmin().thenCompose(admin -> admin
                                            .topics().getPartitionedTopicMetadataAsync(fullTopicName))
                                    .whenComplete((partitionedTopicMetadata, throwable) -> {
                                        if (throwable != null) {
                                            if (throwable instanceof CompletionException
                                                    && throwable.getCause() != null) {
                                                throwable = throwable.getCause();
                                            }
                                            if (throwable instanceof PulsarAdminException.NotAuthorizedException) {
                                                // Failed get partitions due to authorization errors
                                                allTopicMetadata.add(
                                                        new TopicMetadata(
                                                                Errors.TOPIC_AUTHORIZATION_FAILED,
                                                                topic,
                                                                isInternalTopic(fullTopicName),
                                                                Collections.emptyList()));
                                                log.warn("[{}] Request {}: Failed to get partitioned pulsar topic {} "
                                                                + "metadata: {}",
                                                        ctx.channel(), metadataHar.getHeader(),
                                                        fullTopicName, throwable.getMessage());
                                                completeOneTopic.run();
                                                return;
                                            }
                                            if (throwable instanceof PulsarAdminException.NotFoundException) {
                                                if (kafkaConfig.isAllowAutoTopicCreation()
                                                        && metadataRequest.allowAutoTopicCreation()) {
                                                    log.info("[{}] Request {}: Topic {} doesn't exist, "
                                                                    + "auto create it with {} partitions",
                                                            ctx.channel(), metadataHar.getHeader(),
                                                            topic, defaultNumPartitions);
                                                    getPulsarAdmin().thenCompose(admin -> admin
                                                                    .topics().createPartitionedTopicAsync(
                                                                    fullTopicName, defaultNumPartitions))
                                                            .whenComplete((ignored, e) -> {
                                                                if (e == null) {
                                                                    addTopicPartition.accept(topic,
                                                                            defaultNumPartitions);
                                                                } else {
                                                                    log.error("[{}] Failed to create " +
                                                                                    "partitioned topic {}",
                                                                            ctx.channel(), topic, e);
                                                                    completeOneTopic.run();
                                                                }
                                                            });
                                                } else {
                                                    log.error("[{}] Request {}: Topic {} doesn't exist and it's "
                                                                    + "not allowed to auto create partitioned topic",
                                                            ctx.channel(), metadataHar.getHeader(), topic);
                                                    // not allow to auto create topic, return unknown topic
                                                    allTopicMetadata.add(
                                                            new TopicMetadata(
                                                                    Errors.UNKNOWN_TOPIC_OR_PARTITION,
                                                                    topic,
                                                                    isInternalTopic(fullTopicName),
                                                                    Collections.emptyList()));
                                                    completeOneTopic.run();
                                                }
                                            } else {
                                                // Failed get partitions.
                                                allTopicMetadata.add(
                                                        new TopicMetadata(
                                                                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                                                                topic,
                                                                isInternalTopic(fullTopicName),
                                                                Collections.emptyList()));
                                                log.warn("[{}] Request {}: Failed to get partitioned pulsar topic {} "
                                                                + "metadata: {}",
                                                        ctx.channel(), metadataHar.getHeader(),
                                                        fullTopicName, throwable.getMessage());
                                                completeOneTopic.run();
                                            }
                                        } else { // the topic already existed
                                            if (partitionedTopicMetadata.partitions > 0) {
                                                if (log.isDebugEnabled()) {
                                                    log.debug("Topic {} has {} partitions",
                                                            topic, partitionedTopicMetadata.partitions);
                                                }
                                                addTopicPartition.accept(topic, partitionedTopicMetadata.partitions);
                                            } else {
                                                // In case non-partitioned topic, treat as a one partitioned topic.
                                                nonPartitionedTopicMap.put(TopicName
                                                                .get(fullTopicName)
                                                                .getPartition(0)
                                                                .toString(),
                                                        TopicName.get(fullTopicName)
                                                );
                                                addTopicPartition.accept(topic, 1);
                                            }
                                        }
                                    });
                        });

            });
        }

        // 2. After get all topics, for each topic, get the service Broker for it, and add to response
        AtomicInteger topicsCompleted = new AtomicInteger(0);
        // Each Pulsar broker can manage metadata like controller in Kafka, Kafka's AdminClient needs to find a
        // controller node for metadata management. So here we return the broker itself as a controller.
        final int controllerId = newSelfNode().id();
        pulsarTopicsFuture.whenComplete((pulsarTopics, e) -> {
            if (e != null) {
                log.warn("[{}] Request {}: Exception fetching metadata, will return null Response",
                        ctx.channel(), metadataHar.getHeader(), e);
                allNodes.add(newSelfNode());
                MetadataResponse finalResponse =
                        new MetadataResponse(
                                allNodes,
                                clusterName,
                                controllerId,
                                Collections.emptyList());
                resultFuture.complete(finalResponse);
                return;
            }

            final int topicsNumber = pulsarTopics.size();

            if (topicsNumber == 0) {
                // no topic partitions added, return now.
                allNodes.add(newSelfNode());
                MetadataResponse finalResponse =
                        new MetadataResponse(
                                allNodes,
                                clusterName,
                                controllerId,
                                allTopicMetadata);
                resultFuture.complete(finalResponse);
                return;
            }
            pulsarTopics.forEach((topic, list) -> {
                final int partitionsNumber = list.size();
                AtomicInteger partitionsCompleted = new AtomicInteger(0);
                List<PartitionMetadata> partitionMetadatas = Collections
                        .synchronizedList(Lists.newArrayListWithExpectedSize(partitionsNumber));
                list.forEach(topicName -> {
                    // For non-partitioned topic.
                    TopicName realTopicName = nonPartitionedTopicMap.getOrDefault(topicName.toString(), topicName);
                    findBroker(realTopicName)
                            .whenComplete((partitionMetadata, throwable) -> {
                                if (throwable != null || partitionMetadata == null) {
                                    log.warn("[{}] Request {}: Exception while find Broker metadata",
                                            ctx.channel(), metadataHar.getHeader(), throwable);
                                    partitionMetadatas.add(newFailedPartitionMetadata(topicName));
                                } else {
                                    // cache the current owner
                                    Node newNode = partitionMetadata.leader();
                                    log.info("{} For topic {} the leader is {}", this, topicName, newNode);

                                    // answer that we are the owner and that there is only one replice
                                    Node newNodeAnswer = newSelfNode();
                                    partitionMetadata = new PartitionMetadata(Errors.NONE,
                                            partitionMetadata.partition(), newNodeAnswer, Arrays.asList(newNodeAnswer),
                                            Arrays.asList(newNodeAnswer), Collections.emptyList());

                                    synchronized (allNodes) {
                                        if (!allNodes.stream().anyMatch(node1 -> node1.equals(newNodeAnswer))) {
                                            allNodes.add(newNodeAnswer);
                                        }
                                    }
                                    partitionMetadatas.add(partitionMetadata);
                                }

                                // whether completed this topic's partitions list.
                                int finishedPartitions = partitionsCompleted.incrementAndGet();
                                if (log.isTraceEnabled()) {
                                    log.trace("[{}] Request {}: FindBroker for topic {}, partitions found/all: {}/{}.",
                                            ctx.channel(), metadataHar.getHeader(),
                                            topic, finishedPartitions, partitionsNumber);
                                }
                                if (finishedPartitions == partitionsNumber) {
                                    // new TopicMetadata for this topic
                                    allTopicMetadata.add(
                                            new TopicMetadata(
                                                    Errors.NONE,
                                                    // The topic returned to Kafka clients should
                                                    // be the same with what it sent
                                                    topic,
                                                    isInternalTopic(new KopTopic(topic).getFullName()),
                                                    partitionMetadatas));

                                    // whether completed all the topics requests.
                                    int finishedTopics = topicsCompleted.incrementAndGet();
                                    if (log.isTraceEnabled()) {
                                        log.trace("[{}] Request {}: Completed findBroker for topic {}, "
                                                        + "partitions found/all: {}/{}. \n dump All Metadata:",
                                                ctx.channel(), metadataHar.getHeader(), topic,
                                                finishedTopics, topicsNumber);

                                        allTopicMetadata.stream()
                                                .forEach(data ->
                                                        log.trace("TopicMetadata response: {}", data.toString()));
                                    }
                                    if (finishedTopics == topicsNumber) {
                                        // TODO: confirm right value for controller_id
                                        MetadataResponse finalResponse =
                                                new MetadataResponse(
                                                        allNodes,
                                                        clusterName,
                                                        controllerId,
                                                        allTopicMetadata);
                                        resultFuture.complete(finalResponse);
                                    }
                                }
                            });
                });
            });
        });
    }

    protected void handleProduceRequest(KafkaHeaderAndRequest produceHar,
                                        CompletableFuture<AbstractResponse> resultFuture) {
        log.info("handleProduceRequest id {}", produceHar.getHeader().correlationId());
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


        // validate system topics
        for (TopicPartition topicPartition : produceRequest.partitionRecordsOrFail().keySet()) {
            final String fullPartitionName = KopTopic.toString(topicPartition);
            // check KOP inner topic
            if (isOffsetTopic(fullPartitionName) || isTransactionTopic(fullPartitionName)) {
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

        boolean multipleBrokers = false;

        Map<String, PartitionMetadata> brokers = new ConcurrentHashMap<>();
        List<CompletableFuture<?>> lookups = new ArrayList<>(topicPartitionNum.get());
        produceRequest.partitionRecordsOrFail().forEach((topicPartition, records) -> {
            final String fullPartitionName = KopTopic.toString(topicPartition);
            lookups.add(findBroker(TopicName.get(fullPartitionName))
                    .thenAccept(p -> brokers.put(fullPartitionName, p)));
        });
        FutureUtil.waitForAll(lookups)
                .exceptionally((error) -> {
                    // TODO: report errors for specific partitions and continue for non failed lookups
                    Map<TopicPartition, PartitionResponse> errorsMap =
                            produceRequest.partitionRecordsOrFail()
                                    .keySet()
                                    .stream()
                                    .collect(Collectors.toMap(Function.identity(),
                                            p -> new PartitionResponse(Errors.REQUEST_TIMED_OUT)));

                    resultFuture.complete(new ProduceResponse(errorsMap));
                    return null;
                })
                .join();

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
            log.debug("forward FULL produce id {} of {} parts to {}", produceHar.getHeader().correlationId(), numPartitions, broker);
            grabConnectionToBroker(broker.leader().host(), broker.leader().port()).
                    forwardRequest(produceHar)
                    .thenAccept(response -> {
                        ProduceResponse resp = (ProduceResponse) response;
                        resp.responses().forEach((topicPartition, topicResp) -> {
                            if (topicResp.error == Errors.NOT_LEADER_FOR_PARTITION) {
                                String fullTopicName = KopTopic.toString(topicPartition);
                                log.info("Broker {} is no more the leader for {}", broker.leader(), fullTopicName);
                                topicsLeaders.remove(fullTopicName);
                            } else {
                                log.debug("forward FULL produce id {} COMPLETE  of {} parts to {}", produceHar.getHeader().correlationId(), numPartitions, broker);
                            }
                        });
                        resultFuture.complete(response);
                    }).exceptionally(error -> {
                        log.error("Full Produce failed", error);
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

            produceRequest.partitionRecordsOrFail().forEach((topicPartition, records) -> {
                final Consumer<Long> offsetConsumer = offset -> addPartitionResponse.accept(
                        topicPartition, new PartitionResponse(Errors.NONE, offset, -1L, -1L));
                final Consumer<Errors> errorsConsumer =
                        errors -> addPartitionResponse.accept(topicPartition, new PartitionResponse(errors));

                final String fullPartitionName = KopTopic.toString(topicPartition);
                // TODO: have a better way to find an unused correlation id
                int dummyCorrelationId = getDummyCorrelationId();
                Map<TopicPartition, MemoryRecords> recordsCopy = new HashMap<>();
                recordsCopy.put(topicPartition, records);

                RequestHeader header = new RequestHeader(
                        produceHar.getHeader().apiKey(),
                        produceHar.getHeader().apiVersion(),
                        produceHar.getHeader().clientId(),
                        dummyCorrelationId
                );

                ProduceRequest requestForSinglePartition = ProduceRequest.Builder.forCurrentMagic((short) 1,
                                produceRequest.timeout(),
                                recordsCopy)
                        .build();

                ByteBuffer buffer = requestForSinglePartition.serialize(header);

                KafkaHeaderAndRequest singlePartitionRequest = new KafkaHeaderAndRequest(
                        header,
                        requestForSinglePartition,
                        Unpooled.wrappedBuffer(buffer),
                        null
                );

                PartitionMetadata topicMetadata = brokers.get(fullPartitionName);
                Node kopBroker = topicMetadata.leader();
                log.debug("forward produce for {} to {}", fullPartitionName, kopBroker);
                grabConnectionToBroker(kopBroker.host(), kopBroker.port())
                        .forwardRequest(singlePartitionRequest)
                        .thenAccept(response -> {
                            ProduceResponse resp = (ProduceResponse) response;
                            resp.responses().values().forEach(partitionResponse -> {
                                if (partitionResponse.error == Errors.NONE) {
                                    log.debug("result produce for {} to {} {}", fullPartitionName,
                                            kopBroker, partitionResponse);
                                    offsetConsumer.accept(partitionResponse.baseOffset);
                                } else {
                                    if (partitionResponse.error == Errors.NOT_LEADER_FOR_PARTITION) {
                                        log.info("Broker {} is no more the leader for {}", kopBroker, fullPartitionName);
                                        topicsLeaders.remove(fullPartitionName);
                                    }
                                    errorsConsumer.accept(partitionResponse.error);
                                }
                            });
                        }).exceptionally(error -> {
                            log.error("bad error", error);
                            errorsConsumer.accept(Errors.BROKER_NOT_AVAILABLE);
                            return null;
                        });
            });
        }
    }

    private AtomicInteger dummyCorrelationIdGenerator = new AtomicInteger(-1);

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


        Map<TopicPartition, FetchResponse.PartitionData<?>> responseMap = new ConcurrentHashMap<>();
        final AtomicInteger topicPartitionNum = new AtomicInteger(fetchRequest.fetchData().size());

        // validate system topics
        for (TopicPartition topicPartition : fetchRequest.fetchData().keySet()) {
            final String fullPartitionName = KopTopic.toString(topicPartition);
            // check KOP inner topic
            if (isOffsetTopic(fullPartitionName) || isTransactionTopic(fullPartitionName)) {
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

        boolean multipleBrokers = false;

        Map<String, PartitionMetadata> brokers = new ConcurrentHashMap<>();
        List<CompletableFuture<?>> lookups = new ArrayList<>(topicPartitionNum.get());
        fetchRequest.fetchData().forEach((topicPartition, partitionData) -> {
            final String fullPartitionName = KopTopic.toString(topicPartition);
            lookups.add(findBroker(TopicName.get(fullPartitionName))
                    .thenAccept(p -> brokers.put(fullPartitionName, p)));
        });
        FutureUtil.waitForAll(lookups)
                .exceptionally((error) -> {
                    // TODO: report errors for specific partitions and continue for non failed lookups
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
                    return null;
                })
                .join();

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
                    }).exceptionally(error -> {
                        log.error("bad error", error);
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
                    log.debug("[{}] Request {}: Complete handle fetch.", ctx.channel(), fetch.toString());
                }
                final LinkedHashMap<TopicPartition, FetchResponse.PartitionData<?>> responseMapRaw =
                        new LinkedHashMap<>(responseMap);
                resultFuture.complete(new FetchResponse(Errors.NONE,
                        responseMapRaw, 0, fetchRequest.metadata().sessionId()));
            };
            BiConsumer<TopicPartition, FetchResponse.PartitionData> addFetchPartitionResponse
                    = (topicPartition, response) -> {

                responseMap.put(topicPartition, response);
                // reset topicPartitionNum
                int restTopicPartitionNum = topicPartitionNum.decrementAndGet();
                log.debug("addFetchPartitionResponse {} {} restTopicPartitionNum {}", topicPartition, response,
                        restTopicPartitionNum);
                if (restTopicPartitionNum < 0) {
                    return;
                }
                if (restTopicPartitionNum == 0) {
                    complete.run();
                }
            };

            fetchRequest.fetchData().forEach((topicPartition, partitionData) -> {
                final Consumer<FetchResponse.PartitionData> resultConsumer = data -> addFetchPartitionResponse.accept(
                        topicPartition, data);
                final Consumer<Errors> errorsConsumer =
                        errors -> addFetchPartitionResponse.accept(topicPartition,
                                new FetchResponse.PartitionData(errors, 0, 0, 0,
                                        null, MemoryRecords.EMPTY));

                final String fullPartitionName = KopTopic.toString(topicPartition);
                // TODO: have a better way to find an unused correlation id
                int dummyCorrelationId = getDummyCorrelationId();
                Map<TopicPartition, FetchRequest.PartitionData> recordsCopy = new HashMap<>();
                recordsCopy.put(topicPartition, partitionData);

                RequestHeader header = new RequestHeader(
                        fetch.getHeader().apiKey(),
                        fetch.getHeader().apiVersion(),
                        fetch.getHeader().clientId(),
                        dummyCorrelationId
                );

                FetchRequest requestForSinglePartition = FetchRequest.Builder
                        .forConsumer(((FetchRequest) fetch.getRequest()).maxWait(),
                                ((FetchRequest) fetch.getRequest()).minBytes(),
                                recordsCopy)
                        .build();

                ByteBuffer buffer = requestForSinglePartition.serialize(header);

                KafkaHeaderAndRequest singlePartitionRequest = new KafkaHeaderAndRequest(
                        header,
                        requestForSinglePartition,
                        Unpooled.wrappedBuffer(buffer),
                        null
                );

                PartitionMetadata topicMetadata = brokers.get(fullPartitionName);
                Node kopBroker = topicMetadata.leader();
                log.debug("forward fetch for {} to {}", fullPartitionName, kopBroker);
                grabConnectionToBroker(kopBroker.host(), kopBroker.port())
                        .forwardRequest(singlePartitionRequest)
                        .thenAccept(response -> {
                            FetchResponse<?> resp = (FetchResponse) response;
                            resp.responseData()
                                .forEach((part, partitionResponse) -> {
                                    if (partitionResponse.error == Errors.NOT_LEADER_FOR_PARTITION) {
                                        String fullTopicName = KopTopic.toString(topicPartition);
                                        log.info("Broker {} is no more the leader for {}", kopBroker, fullTopicName);
                                        topicsLeaders.remove(fullTopicName);
                                    }
                                    log.debug("result fetch for {} to {} {}", fullPartitionName, kopBroker,
                                            partitionResponse);
                                    resultConsumer.accept(partitionResponse);
                                });
                        }).exceptionally(error -> {
                            log.error("bad error", error);
                            errorsConsumer.accept(Errors.UNKNOWN_SERVER_ERROR);
                            return null;
                        });
            });
        }
    }

    private CompletableFuture<PartitionMetadata> findCoordinator(FindCoordinatorRequest.CoordinatorType type,
                                                                 String key) {
        String pulsarTopicName = computePulsarTopicName(type, key);
        log.debug("findCoordinator for {} {} -> topic {}", type, key, pulsarTopicName);
        return findBroker(TopicName.get(pulsarTopicName), true);
    }

    private String computePulsarTopicName(FindCoordinatorRequest.CoordinatorType type, String key) {
        String pulsarTopicName;
        int partition;

        if (type == FindCoordinatorRequest.CoordinatorType.TRANSACTION) {
            TransactionConfig transactionConfig = TransactionConfig.builder()
                    .transactionLogNumPartitions(kafkaConfig.getTxnLogTopicNumPartitions())
                    .transactionMetadataTopicName(MetadataUtils.constructTxnLogTopicBaseName(kafkaConfig))
                    .build();
            partition = TransactionCoordinator.partitionFor(key, kafkaConfig.getTxnLogTopicNumPartitions());
            pulsarTopicName = TransactionCoordinator
                    .getTopicPartitionName(transactionConfig.getTransactionMetadataTopicName(), partition);
        } else if (type == FindCoordinatorRequest.CoordinatorType.GROUP) {
            partition = GroupMetadataManager.getPartitionId(key, kafkaConfig.getOffsetsTopicNumPartitions());
            pulsarTopicName = GroupMetadataManager
                    .getTopicPartitionName(kafkaConfig.getKafkaMetadataTenant() + "/"
                    + kafkaConfig.getKafkaMetadataNamespace()
                    + "/" + Topic.GROUP_METADATA_TOPIC_NAME, partition);
        } else {
            throw new NotImplementedException("FindCoordinatorRequest not support TRANSACTION type " + type);
        }
        return pulsarTopicName;
    }

    protected void handleFindCoordinatorRequest(KafkaHeaderAndRequest findCoordinator,
                                                CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(findCoordinator.getRequest() instanceof FindCoordinatorRequest);
        FindCoordinatorRequest request = (FindCoordinatorRequest) findCoordinator.getRequest();

        // we are always the coordinator!
        AbstractResponse response = new FindCoordinatorResponse(
                Errors.NONE,
                newSelfNode());
        resultFuture.complete(response);

    }

    protected void handleJoinGroupRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        JoinGroupRequest joinGroupRequest = (JoinGroupRequest) kafkaHeaderAndRequest.getRequest();
        log.info("handleJoinGroupRequest {}", joinGroupRequest.groupId());

        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                JoinGroupRequest.class, JoinGroupRequest::groupId,
                (JoinGroupRequest request) -> {
                    return new JoinGroupResponse(
                            Errors.BROKER_NOT_AVAILABLE,
                            0,
                            "",
                            "",
                            "", new HashMap<>()
                    );
                });
    }

    protected void handleSyncGroupRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                          CompletableFuture<AbstractResponse> resultFuture) {

        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                SyncGroupRequest.class, SyncGroupRequest::groupId,
                (SyncGroupRequest request) -> {
                    return new SyncGroupResponse(
                            Errors.BROKER_NOT_AVAILABLE,
                            null
                    );
                });
    }

    protected void handleHeartbeatRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                          CompletableFuture<AbstractResponse> resultFuture) {

        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                HeartbeatRequest.class, HeartbeatRequest::groupId,
                (HeartbeatRequest request) -> {
                    return new HeartbeatResponse(0,
                            Errors.BROKER_NOT_AVAILABLE
                    );
                });
    }

    @Override
    protected void handleLeaveGroupRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                           CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                LeaveGroupRequest.class, LeaveGroupRequest::groupId,
                (LeaveGroupRequest request) -> {
                    return new LeaveGroupResponse(Errors.BROKER_NOT_AVAILABLE);
                });
    }

    @Override
    protected void handleDescribeGroupRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                              CompletableFuture<AbstractResponse> resultFuture) {
        // forward to the coordinator of the first group
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                DescribeGroupsRequest.class,
                (DescribeGroupsRequest r) -> r.groupIds().get(0),
                (DescribeGroupsRequest request) -> {
                    Map<String, DescribeGroupsResponse.GroupMetadata> errors = request.groupIds().stream()
                            .collect(Collectors.toMap(Function.identity(), gid -> {
                        return new DescribeGroupsResponse.GroupMetadata(Errors.BROKER_NOT_AVAILABLE, null,
                                null, null, Collections.emptyList());
                    }));
                    DescribeGroupsResponse res = new DescribeGroupsResponse(errors);
                    return res;
                });
    }

    @Override
    protected void handleListGroupsRequest(KafkaHeaderAndRequest listGroups,
                                           CompletableFuture<AbstractResponse> resultFuture) {
        ListGroupsResponse response = new ListGroupsResponse(Errors.BROKER_NOT_AVAILABLE, new ArrayList<>());
        resultFuture.complete(response);
    }

    @Override
    protected void handleDeleteGroupsRequest(KafkaHeaderAndRequest deleteGroups,
                                             CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(deleteGroups.getRequest() instanceof DeleteGroupsRequest);
        DeleteGroupsRequest request = (DeleteGroupsRequest) deleteGroups.getRequest();

        Map<String, Errors> deleteResult = new HashMap<>();
        request.groups().forEach(k -> {
            deleteResult.put(k, Errors.BROKER_NOT_AVAILABLE);
        });
        DeleteGroupsResponse response = new DeleteGroupsResponse(
                deleteResult
        );
        resultFuture.complete(response);
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
        checkArgument(createTopics.getRequest() instanceof CreateTopicsRequest);
        CreateTopicsRequest request = (CreateTopicsRequest) createTopics.getRequest();
        if (request.validateOnly()) {
            Map<String, ApiError> errors = request
                    .topics()
                    .keySet()
                    .stream()
                    .collect(Collectors.toMap(Function.identity(), (a) ->
                            ApiError.fromThrowable(new UnsupportedOperationException())));
            resultFuture.complete(new CreateTopicsResponse(errors));
            return;
        }

        final Map<String, ApiError> result = new HashMap<>();
        final Map<String, CreateTopicsRequest.TopicDetails> validTopics = new HashMap<>();
        final Set<String> duplicateTopics = request.duplicateTopics();

        request.topics().forEach((topic, details) -> {
            if (!duplicateTopics.contains(topic)) {
                validTopics.put(topic, details);
            } else {
                final String errorMessage = "Create topics request from client `" + createTopics.getHeader().clientId()
                        + "` contains multiple entries for the following topics: " + duplicateTopics;
                result.put(topic, new ApiError(Errors.INVALID_REQUEST, errorMessage));
            }
        });

        if (validTopics.isEmpty()) {
            resultFuture.complete(new CreateTopicsResponse(result));
        } else {
            // we are using a PulsarAdmin with the user identity, so Pulsar will handle authorization
            getPulsarAdmin().thenCompose(admin -> {
                AdminManager adminManager = new AdminManager(admin, kafkaConfig);
                return adminManager.createTopicsAsync(validTopics, request.timeout()).thenApply(validResult -> {
                    result.putAll(validResult);
                    resultFuture.complete(new CreateTopicsResponse(result));
                    return null;
                });
            }).exceptionally(error -> {
                Map<String, ApiError> errors = request
                        .topics()
                        .keySet()
                        .stream()
                        .collect(Collectors.toMap(Function.identity(), (a) -> ApiError.fromThrowable(error)));
                resultFuture.complete(new CreateTopicsResponse(errors));
                return null;
            });
        }
    }

    protected void handleDescribeConfigs(KafkaHeaderAndRequest describeConfigs,
                                         CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(describeConfigs.getRequest() instanceof DescribeConfigsRequest);
        DescribeConfigsRequest request = (DescribeConfigsRequest) describeConfigs.getRequest();

        getPulsarAdmin().thenCompose(admin -> {
            AdminManager adminManager = new AdminManager(admin, kafkaConfig);
            return adminManager.describeConfigsAsync(new ArrayList<>(request.resources()).stream()
                    .collect(Collectors.toMap(
                            resource -> resource,
                            resource -> Optional.ofNullable(request.configNames(resource)).map(HashSet::new)
                    ))
            ).thenApply(configResourceConfigMap -> {
                resultFuture.complete(new DescribeConfigsResponse(0, configResourceConfigMap));
                return null;
            });
        }).exceptionally(error -> {
            Map<ConfigResource, DescribeConfigsResponse.Config> errors = request
                    .resources()
                    .stream()
                    .collect(Collectors.toMap(Function.identity(), r -> {
                        return new DescribeConfigsResponse.Config(ApiError.fromThrowable(error),
                                Collections.emptyList());
                    }));
            resultFuture.complete(new DescribeConfigsResponse(0, errors));
            return null;
        });
        ;
    }

    @Override
    protected void handleDeleteTopics(KafkaHeaderAndRequest deleteTopics,
                                      CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(deleteTopics.getRequest() instanceof DeleteTopicsRequest);
        DeleteTopicsRequest request = (DeleteTopicsRequest) deleteTopics.getRequest();
        Set<String> topicsToDelete = request.topics();
        getPulsarAdmin(false).thenApply(admin -> {
            AdminManager adminManager = new AdminManager(admin, kafkaConfig);
            resultFuture.complete(new DeleteTopicsResponse(adminManager.deleteTopics(topicsToDelete)));
            return null;
        }).exceptionally(error -> {
            Map<String, Errors> errors = request
                    .topics()
                    .stream()
                    .collect(Collectors.toMap(Function.identity(), (a) -> Errors.forException(error)));
            resultFuture.complete(new DeleteTopicsResponse(errors));
            return null;
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

            // getTopicBroker returns null. topic should be removed from LookupCache.
            KafkaTopicManager.removeTopicManagerCache(topic.toString());

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

    protected boolean isOffsetTopic(String topic) {
        String offsetsTopic = kafkaConfig.getKafkaMetadataTenant() + "/"
                + kafkaConfig.getKafkaMetadataNamespace()
                + "/" + GROUP_METADATA_TOPIC_NAME;

        return topic != null && topic.contains(offsetsTopic);
    }

    protected boolean isTransactionTopic(String topic) {
        String transactionTopic = kafkaConfig.getKafkaMetadataTenant() + "/"
                + kafkaConfig.getKafkaMetadataNamespace()
                + "/" + TRANSACTION_STATE_TOPIC_NAME;

        return topic != null && topic.contains(transactionTopic);
    }

    private CompletableFuture<PulsarAdmin> getPulsarAdmin() {
        return getPulsarAdmin(false);
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
        getPulsarAdmin(system).thenCompose(admin -> admin.lookups().lookupTopicAsync(topic.toString())
                .thenCompose(address -> getProtocolDataToAdvertise(address, topic))
                .whenComplete((stringOptional, throwable) -> {
                    if (throwable != null || stringOptional == null || !stringOptional.isPresent()) {
                        log.error("Not get advertise data for Kafka topic:{}. throwable",
                                topic, throwable);
                        KafkaTopicManager.removeTopicManagerCache(topic.toString());
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

                    // here we found topic broker: broker2, but this is in broker1,
                    // how to clean the lookup cache?
                    if (!advertisedListeners.contains(endPoint.getOriginalListener())) {
                        KafkaTopicManager.removeTopicManagerCache(topic.toString());
                    }
                    topicsLeaders.put(topic.toString(), node);
                    returnFuture.complete(newPartitionMetadata(topic, node));
                }).exceptionally(error -> {
                    log.error("bad error", error);
                    return null;
                }));
        return returnFuture;
    }

    static Node newNode(InetSocketAddress address) {
        if (log.isTraceEnabled()) {
            log.trace("Return Broker Node of {}. {}:{}", address, address.getHostString(), address.getPort());
        }
        return new Node(
                Murmur3_32Hash.getInstance().makeHash((address.getHostString() + address.getPort()).getBytes(UTF_8)),
                address.getHostString(),
                address.getPort());
    }

    Node newSelfNode() {
        return newNode(advertisedEndPoint.getInetAddress());
    }

    static PartitionMetadata newPartitionMetadata(TopicName topicName, Node node) {
        int pulsarPartitionIndex = topicName.getPartitionIndex();
        int kafkaPartitionIndex = pulsarPartitionIndex == -1 ? 0 : pulsarPartitionIndex;

        if (log.isTraceEnabled()) {
            log.trace("Return PartitionMetadata node: {}, topicName: {}", node, topicName);
        }

        return new PartitionMetadata(
                Errors.NONE,
                kafkaPartitionIndex,
                node,                      // leader
                Lists.newArrayList(node),  // replicas
                Lists.newArrayList(node),  // isr
                Collections.emptyList()     // offline replicas
        );
    }

    static PartitionMetadata newFailedPartitionMetadata(TopicName topicName) {
        int pulsarPartitionIndex = topicName.getPartitionIndex();
        int kafkaPartitionIndex = pulsarPartitionIndex == -1 ? 0 : pulsarPartitionIndex;

        log.warn("Failed find Broker metadata, create PartitionMetadata with NOT_LEADER_FOR_PARTITION");

        // most of this error happens when topic is in loading/unloading status,
        return new PartitionMetadata(
                Errors.NOT_LEADER_FOR_PARTITION,
                kafkaPartitionIndex,
                Node.noNode(),                      // leader
                Lists.newArrayList(Node.noNode()),  // replicas
                Lists.newArrayList(Node.noNode()),  // isr
                Collections.emptyList()             // offline replicas
        );
    }

    static AbstractResponse failedResponse(KafkaHeaderAndRequest requestHar, Throwable e) {
        if (log.isDebugEnabled()) {
            log.debug("Request {} get failed response ", requestHar.getHeader().apiKey(), e);
        }
        return requestHar.getRequest().getErrorResponse(((Integer) THROTTLE_TIME_MS.defaultValue), e);
    }

    @VisibleForTesting
    protected CompletableFuture<Boolean> authorize(AclOperation operation, Resource resource) {
        if (authorizer == null) {
            return CompletableFuture.completedFuture(true);
        }
        if (authenticator.session() == null) {
            return CompletableFuture.completedFuture(false);
        }

        CompletableFuture<Boolean> isAuthorizedFuture;
        Session session = authenticator.session();
        switch (operation) {
            case READ:
                isAuthorizedFuture = authorizer.canConsumeAsync(session.getPrincipal(), resource);
                break;
            case IDEMPOTENT_WRITE:
            case WRITE:
                isAuthorizedFuture = authorizer.canProduceAsync(session.getPrincipal(), resource);
                break;
            case DESCRIBE:
                isAuthorizedFuture = authorizer.canLookupAsync(session.getPrincipal(), resource);
                break;
            case CREATE:
            case DELETE:
            case CLUSTER_ACTION:
            case DESCRIBE_CONFIGS:
            case ALTER_CONFIGS:
            case ALTER:
            case UNKNOWN:
            case ALL:
            case ANY:
            default:
                return FutureUtil.failedFuture(
                        new IllegalStateException("AclOperation [" + operation.name() + "] is not supported."));
        }
        return isAuthorizedFuture;
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
        checkArgument(listOffset.getRequest() instanceof ListOffsetRequest);
        ListOffsetRequest request = (ListOffsetRequest) listOffset.getRequest();

        Map<TopicPartition, ListOffsetResponse.PartitionData> map = new ConcurrentHashMap<>();
        AtomicInteger expectedCount = new AtomicInteger(request.offsetData().size());

        BiConsumer<String, ListOffsetResponse> onResponse = (topic, topicResponse) -> {
            topicResponse.responseData().forEach((tp, data) -> {
                map.put(tp, data);
                if (expectedCount.decrementAndGet() == 0) {
                    ListOffsetResponse response = new ListOffsetResponse(map);
                    resultFuture.complete(response);
                }
            });
        };

        for (Map.Entry<TopicPartition, ListOffsetRequest.PartitionData> entry : request.offsetData().entrySet()) {
            final String fullPartitionName = KopTopic.toString(entry.getKey());

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
                    .setOffsetData(tsData)
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
                                                    0, 0));
                                    onResponse.accept(fullPartitionName, dummyResponse);
                                    return null;
                                });
                    }).exceptionally(err -> {
                        ListOffsetResponse dummyResponse = new ListOffsetResponse(new HashMap<>());
                        dummyResponse.responseData().put(entry.getKey(),
                                new ListOffsetResponse.PartitionData(Errors.BROKER_NOT_AVAILABLE,
                                        0, 0));
                        onResponse.accept(fullPartitionName, dummyResponse);
                        return null;
                    });
        }
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

        for (Map.Entry<TopicPartition, Long> entry : request.partitionTimestamps().entrySet()) {
            final String fullPartitionName = KopTopic.toString(entry.getKey());

            int dummyCorrelationId = getDummyCorrelationId();
            RequestHeader header = new RequestHeader(
                    listOffset.getHeader().apiKey(),
                    listOffset.getHeader().apiVersion(),
                    listOffset.getHeader().clientId(),
                    dummyCorrelationId
            );

            Map<TopicPartition, Long> tsData = new HashMap<>();
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
                                                    0, 0));
                                    onResponse.accept(fullPartitionName, dummyResponse);
                                    return null;
                                });
                    }).exceptionally(err -> {
                        ListOffsetResponse dummyResponse = new ListOffsetResponse(new HashMap<>());
                        dummyResponse.responseData().put(entry.getKey(),
                                new ListOffsetResponse.PartitionData(Errors.BROKER_NOT_AVAILABLE,
                                        0, 0));
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
                (OffsetFetchRequest request) -> {
                    return new OffsetFetchResponse(Errors.BROKER_NOT_AVAILABLE, new HashMap<>());
                });
    }

    @Override
    protected void handleOffsetCommitRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                             CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture,
                FindCoordinatorRequest.CoordinatorType.GROUP, OffsetCommitRequest.class, OffsetCommitRequest::groupId,
                (OffsetCommitRequest request) -> {
                    Map<TopicPartition, Errors> map = request.offsetData().keySet().stream()
                            .collect(Collectors.toMap(Function.identity(), i -> Errors.BROKER_NOT_AVAILABLE));
                    return new OffsetCommitResponse(0, map);
                });
    }

    private <K extends AbstractRequest, R extends AbstractResponse> void handleRequestWithCoordinator(
            KafkaHeaderAndRequest kafkaHeaderAndRequest,
            CompletableFuture<AbstractResponse> resultFuture,
            FindCoordinatorRequest.CoordinatorType coordinatorType,
            Class<K> requestClass,
            Function<K, String> keyExtractor,
            Function<K, R> errorBuilder
    ) {
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
                                    log.info("Sending {} {} errors {}.", serverResponse, serverResponse.getClass(),
                                            serverResponse.errorCounts());
                                }
                                if (serverResponse.errorCounts() != null) {
                                    for (Errors error : serverResponse.errorCounts().keySet()) {
                                        if (error == Errors.NOT_COORDINATOR) {
                                            forgetMetadataForFailedBroker(metadata.leader().host(),
                                                    metadata.leader().port());
                                        }
                                    }
                                }
                                resultFuture.complete(serverResponse);
                            }).exceptionally(err -> {
                                resultFuture.complete(errorBuilder.apply(request));
                                return null;
                                });
                    })
                    .exceptionally((err) -> {
                        resultFuture.complete(errorBuilder.apply(request));
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
                InitProducerIdRequest::transactionalId,
                (InitProducerIdRequest request) -> {
                    return new InitProducerIdResponse(0, Errors.BROKER_NOT_AVAILABLE);
                });
    }

    @Override
    protected void handleAddPartitionsToTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                            CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture,
                FindCoordinatorRequest.CoordinatorType.TRANSACTION, AddPartitionsToTxnRequest.class,
                AddPartitionsToTxnRequest::transactionalId,
                (AddPartitionsToTxnRequest request) -> {
                    Map<TopicPartition, Errors> map = request.partitions().stream()
                            .collect(Collectors.toMap(Function.identity(), i -> Errors.BROKER_NOT_AVAILABLE));
                    return new AddPartitionsToTxnResponse(0, map);
                });
    }

    @Override
    protected void handleAddOffsetsToTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                         CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture,
                FindCoordinatorRequest.CoordinatorType.TRANSACTION, AddOffsetsToTxnRequest.class,
                AddOffsetsToTxnRequest::transactionalId,
                (AddOffsetsToTxnRequest request) -> {
                    // TODO: verify contents of the Map
                    return new AddPartitionsToTxnResponse(0, new HashMap<>());
                });

    }

    @Override
    protected void handleTxnOffsetCommit(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                         CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP,
                TxnOffsetCommitRequest.class, TxnOffsetCommitRequest::consumerGroupId,
                (TxnOffsetCommitRequest request) -> {
                    // TODO: verify contents of the Map
                    return new TxnOffsetCommitResponse(0, new HashMap<>());
                });
    }

    @Override
    protected void handleEndTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture,
                FindCoordinatorRequest.CoordinatorType.TRANSACTION, EndTxnRequest.class, EndTxnRequest::transactionalId,
                (EndTxnRequest request) -> {
                    return new EndTxnResponse(0, Errors.BROKER_NOT_AVAILABLE);
                });
    }

    @Override
    protected void handleWriteTxnMarkers(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                         CompletableFuture<AbstractResponse> resultFuture) {
        resultFuture.completeExceptionally(new UnsupportedOperationException("not a proxy operation"));
    }

    private ConcurrentHashMap<String, ConnectionToBroker> connectionsToBrokers = new ConcurrentHashMap<>();

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

}
