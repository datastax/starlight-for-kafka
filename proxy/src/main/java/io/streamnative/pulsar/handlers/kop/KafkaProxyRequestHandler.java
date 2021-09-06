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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
import org.apache.commons.lang3.NotImplementedException;

import io.netty.buffer.ByteBufUtil;
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
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.RecordBatch;
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
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;

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

    private final String id;

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

    private Function<String, String> DEFAULT_BROKER_ADDRESS_MAPPER = (pulsarAddress -> {
        // The Mapping to the KOP port is done per-convention if you do not have access to Broker Discovery Service.
        // This saves us from a Metadata lookup hop
        String kafkaAddress = pulsarAddress
                .replace("pulsar://", "PLAINTEXT://")
                .replace("pulsar+ssl://", "SSL://");

        if (kafkaConfig.getKafkaProxyBrokerPortToKopMapping() != null) {
            String[] split = kafkaConfig.getKafkaProxyBrokerPortToKopMapping().split(",");
            for (String mapping : split) {
                String[] mappingSplit = mapping.split("=");
                if (mappingSplit.length == 2) {
                    kafkaAddress = kafkaAddress.replace(mappingSplit[0].trim(), mappingSplit[1].trim());
                }
            }
        } else {

            // standard mapping
            kafkaAddress= kafkaAddress.replace("6650", "9092")
                    .replace("6651", "9093")
                    .replace("6652", "9094")
                    .replace("6653", "9095");
        }
        return kafkaAddress;
    });

    public KafkaProxyRequestHandler(String id, KafkaProtocolProxyMain.PulsarAdminProvider pulsarAdmin,
                               AuthenticationService authenticationService,
                               KafkaServiceConfiguration kafkaConfig,
                               boolean tlsEnabled,
                               EndPoint advertisedEndPoint,
                               Function<String, String> brokerAddressMapper) throws Exception {
        super(NullStatsLogger.INSTANCE, kafkaConfig);
        this.brokerAddressMapper = brokerAddressMapper != null ? brokerAddressMapper : DEFAULT_BROKER_ADDRESS_MAPPER;
        this.id = id;
        String auth = kafkaConfig.getBrokerClientAuthenticationPlugin();
        String authParams = kafkaConfig.getBrokerClientAuthenticationParameters();
        this.authenticationToken = AuthenticationUtil.create(auth, authParams);

        this.clusterName = kafkaConfig.getClusterName();
        this.executor = Executors.newScheduledThreadPool(4);
        this.admin = pulsarAdmin;
        final boolean authenticationEnabled = kafkaConfig.isAuthenticationEnabled();
        this.authenticator = authenticationEnabled
                ? new SaslAuthenticator(null, authenticationService, kafkaConfig.getSaslAllowedMechanisms(), kafkaConfig)
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
        System.out.println("handleApiVersionsRequest");
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
        if (unsupportedApiVersion){
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
            getPulsarAdmin().thenCompose(admin -> admin.topics().getPartitionedTopicListAsync(NamespaceName.get(namespace).getLocalName()))
                    .whenComplete((topics, e) -> {
                        if (e != null) {
                            log.error("Failed to getListOfPersistentTopic of {}", namespace, e);
                            topicMapFuture.completeExceptionally(e);
                            return;
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
        System.out.println("handleTopicMetadataRequest "+metadataRequest.topics());
        log.info("[{}] Request {}: for topic {} ",
                ctx.channel(), metadataHar.getHeader(), metadataRequest.topics());


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
                System.out.println("fullTopicName "+fullTopicName);

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
                        getPulsarAdmin().thenCompose(admin -> admin.topics().getPartitionedTopicMetadataAsync(fullTopicName))
                                .whenComplete((partitionedTopicMetadata, throwable) -> {
                                    log.info("getPartitionedTopicMetadataAsync for "+fullTopicName+" -> "+partitionedTopicMetadata, throwable);
                                    if (throwable != null) {
                                        if (throwable instanceof CompletionException
                                                && throwable.getCause() != null) {
                                            throwable = throwable.getCause();
                                        }
                                        if (throwable instanceof PulsarAdminException.NotFoundException) {
                                            if (kafkaConfig.isAllowAutoTopicCreation()
                                                    && metadataRequest.allowAutoTopicCreation()) {
                                                log.info("[{}] Request {}: Topic {} doesn't exist, "
                                                                + "auto create it with {} partitions",
                                                        ctx.channel(), metadataHar.getHeader(),
                                                        topic, defaultNumPartitions);
                                                getPulsarAdmin().thenCompose(admin -> admin.topics().createPartitionedTopicAsync(
                                                                fullTopicName, defaultNumPartitions))
                                                        .whenComplete((ignored, e) -> {
                                                            if (e == null) {
                                                                addTopicPartition.accept(topic, defaultNumPartitions);
                                                            } else {
                                                                log.error("[{}] Failed to create partitioned topic {}",
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
                                log.info("{} For topic {} the leader is {} topicsLeaders {}", this, topicName, newNode, topicsLeaders);


                                // answer that we are the owner and that there is only one replice
                                Node newNodeAnswer = newSelfNode();
                                partitionMetadata = new PartitionMetadata(Errors.NONE,  partitionMetadata.partition(), newNodeAnswer, Arrays.asList(newNodeAnswer), Arrays.asList(newNodeAnswer), Collections.emptyList());

                                synchronized (allNodes) {
                                    if (!allNodes.stream().anyMatch(node1 -> node1.equals(newNodeAnswer))) {
                                        allNodes.add(newNodeAnswer);
                                    }
                                }
                                partitionMetadatas.add(partitionMetadata);
                            }

                            // whether completed this topic's partitions list.
                            int finishedPartitions = partitionsCompleted.incrementAndGet();
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Request {}: FindBroker for topic {}, partitions found/all: {}/{}.",
                                    ctx.channel(), metadataHar.getHeader(),
                                    topic, finishedPartitions, partitionsNumber);
                            }
                            if (finishedPartitions == partitionsNumber) {
                                // new TopicMetadata for this topic
                                allTopicMetadata.add(
                                    new TopicMetadata(
                                        Errors.NONE,
                                        // The topic returned to Kafka clients should be the same with what it sent
                                        topic,
                                        isInternalTopic(new KopTopic(topic).getFullName()),
                                        partitionMetadatas));

                                // whether completed all the topics requests.
                                int finishedTopics = topicsCompleted.incrementAndGet();
                                if (log.isDebugEnabled()) {
                                    log.debug("[{}] Request {}: Completed findBroker for topic {}, "
                                            + "partitions found/all: {}/{}. \n dump All Metadata:",
                                        ctx.channel(), metadataHar.getHeader(), topic,
                                        finishedTopics, topicsNumber);

                                    allTopicMetadata.stream()
                                        .forEach(data -> log.debug("TopicMetadata response: {}", data.toString()));
                                }
                                if (finishedTopics == topicsNumber) {
                                    // TODO: confirm right value for controller_id
                                    MetadataResponse finalResponse =
                                        new MetadataResponse(
                                            allNodes,
                                            clusterName,
                                            controllerId,
                                            allTopicMetadata);
                                    log.info("Metadata response {}", finalResponse);
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
            if (isOffsetTopic(fullPartitionName) || isTransactionTopic(fullPartitionName))
            {
                log.error("[{}] Request {}: not support produce message to inner topic. topic: {}",
                        ctx.channel(), produceHar.getHeader(), topicPartition);
                Map<TopicPartition, PartitionResponse> errorsMap =
                        produceRequest.partitionRecordsOrFail()
                                .keySet()
                                .stream()
                                .collect(Collectors.toMap(Function.identity(), p -> new PartitionResponse(Errors.INVALID_TOPIC_EXCEPTION)));
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
                                    .collect(Collectors.toMap(Function.identity(), p -> new PartitionResponse(Errors.REQUEST_TIMED_OUT)));

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
            log.debug("forward FULL produce of {} parts to {}", numPartitions, broker);
            grabConnectionToBroker(broker.leader().host(), broker.leader().port()).
                    forwardRequest(produceHar)
                    .thenAccept(response -> {
                        ProduceResponse resp = (ProduceResponse) response;
                        resp.responses().forEach( (topicPartition, topicResp) -> {
                            if (topicResp.error == Errors.NOT_LEADER_FOR_PARTITION) {
                                String fullTopicName = KopTopic.toString(topicPartition);
                                log.info("Broker {} is no more the leader for {}", broker.leader(), fullTopicName);
                                topicsLeaders.remove(fullTopicName);
                            }
                        });
                        resultFuture.complete(response);
                    }).exceptionally(error -> {
                        log.error("bad error", error);
                        Map<TopicPartition, PartitionResponse> errorsMap =
                                produceRequest.partitionRecordsOrFail()
                                        .keySet()
                                        .stream()
                                        .collect(Collectors.toMap(Function.identity(), p -> new PartitionResponse(Errors.REQUEST_TIMED_OUT)));
                        resultFuture.complete(new ProduceResponse(errorsMap));
                        return null;
                    });
        } else {
            log.debug("Split produce of {} parts to {}", numPartitions, brokers);
            // we have to create multiple ProduceRequest
            // this is a prototype, let's create a ProduceRequest per each partition
            // we could group requests per broker

            Runnable complete = () -> {
                log.debug("complete produde {}", produceHar);
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
                log.debug("addPartitionResponse {} {} restTopicPartitionNum {}", topicPartition, response, restTopicPartitionNum);
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
                final Consumer<Throwable> exceptionConsumer =
                        e -> addPartitionResponse.accept(topicPartition, new PartitionResponse(Errors.forException(e)));

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
                                    log.debug("result produce for {} to {} {}", fullPartitionName, kopBroker, partitionResponse);
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

    private int getDummyCorrelationId() {
        return new Random().nextInt();
    }

    protected void handleFetchRequest(KafkaHeaderAndRequest fetch,
                                        CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(fetch.getRequest() instanceof FetchRequest);
        FetchRequest fetchRequest = (FetchRequest) fetch.getRequest();

        final int numPartitions = fetchRequest.fetchData().size();
        if (numPartitions == 0) {
            resultFuture.complete(new FetchResponse(Errors.NONE, new LinkedHashMap<>(), 0, fetchRequest.metadata().sessionId()));
            return;
        }


        final LinkedHashMap<TopicPartition, FetchResponse.PartitionData<?>> responseMapRaw = new LinkedHashMap<>();
        Map<TopicPartition, FetchResponse.PartitionData<?>> responseMap = Collections.synchronizedMap(responseMapRaw);
        final AtomicInteger topicPartitionNum = new AtomicInteger(fetchRequest.fetchData().size());

        // validate system topics
        for (TopicPartition topicPartition : fetchRequest.fetchData().keySet()) {
            final String fullPartitionName = KopTopic.toString(topicPartition);
            // check KOP inner topic
            if (isOffsetTopic(fullPartitionName) || isTransactionTopic(fullPartitionName))
            {
                log.error("[{}] Request {}: not support fetch message to inner topic. topic: {}",
                        ctx.channel(), fetch.getHeader(), topicPartition);
                Map<TopicPartition, FetchResponse.PartitionData<?>> errorsMap =
                        fetchRequest.fetchData()
                                .keySet()
                                .stream()
                                .collect(Collectors.toMap(Function.identity(),
                                        p -> new FetchResponse.PartitionData(Errors.INVALID_TOPIC_EXCEPTION,
                                                0,0, 0, null, null)));
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
                                            p -> new FetchResponse.PartitionData(Errors.REQUEST_TIMED_OUT,
                                                    0,0, 0, null, null)));
                    resultFuture.complete(new FetchResponse(Errors.REQUEST_TIMED_OUT,
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
                                                p -> new FetchResponse.PartitionData(Errors.REQUEST_TIMED_OUT,
                                                        0,0, 0, null, null)));
                        resultFuture.complete(new FetchResponse(Errors.REQUEST_TIMED_OUT,
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
                        responseMap.put(topicPartition, new FetchResponse.PartitionData(Errors.REQUEST_TIMED_OUT,
                                0,0, 0, null, null));
                    }
                });
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Request {}: Complete handle fetch.", ctx.channel(), fetch.toString());
                }
                resultFuture.complete(new FetchResponse(Errors.NONE,
                        responseMapRaw, 0, fetchRequest.metadata().sessionId()));
            };
            BiConsumer<TopicPartition, FetchResponse.PartitionData> addPartitionResponse = (topicPartition, response) -> {

                responseMap.put(topicPartition, response);
                // reset topicPartitionNum
                int restTopicPartitionNum = topicPartitionNum.decrementAndGet();
                log.debug("addPartitionResponse {} {} restTopicPartitionNum {}", topicPartition, response, restTopicPartitionNum);
                if (restTopicPartitionNum < 0) {
                    return;
                }
                if (restTopicPartitionNum == 0) {
                    complete.run();
                }
            };

            fetchRequest.fetchData().forEach((topicPartition, partitionData) -> {
                final Consumer<FetchResponse.PartitionData> resultConsumer = data -> addPartitionResponse.accept(
                        topicPartition, data);
                final Consumer<Errors> errorsConsumer =
                        errors -> addPartitionResponse.accept(topicPartition, new FetchResponse.PartitionData(errors, 0,0, 0, null, null));

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

                FetchRequest requestForSinglePartition = FetchRequest.Builder.forConsumer(((FetchRequest) fetch.getRequest()).maxWait(),
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
                                    .forEach( (part, partitionResponse) -> {
                                log.debug("result fetch for {} to {} {}", fullPartitionName, kopBroker, partitionResponse);
                                resultConsumer.accept(partitionResponse);
                            });
                        }).exceptionally(error -> {
                            log.error("bad error", error);
                            errorsConsumer.accept(Errors.BROKER_NOT_AVAILABLE);
                            return null;
                        });
            });
        }
    }

    private CompletableFuture<PartitionMetadata> findCoordinator(FindCoordinatorRequest.CoordinatorType type, String key) {
        String pulsarTopicName;
        int partition;

        if (type == FindCoordinatorRequest.CoordinatorType.TRANSACTION) {
            TransactionConfig transactionConfig = TransactionConfig.builder()
                    .transactionLogNumPartitions(kafkaConfig.getTxnLogTopicNumPartitions())
                    .transactionMetadataTopicName(MetadataUtils.constructTxnLogTopicBaseName(kafkaConfig))
                    .build();
            partition = TransactionCoordinator.partitionFor(key, kafkaConfig.getTxnLogTopicNumPartitions());
            pulsarTopicName = TransactionCoordinator.getTopicPartitionName(transactionConfig.getTransactionMetadataTopicName(), partition);
        } else if (type == FindCoordinatorRequest.CoordinatorType.GROUP) {
            partition = GroupMetadataManager.getPartitionId(key, kafkaConfig.getOffsetsTopicNumPartitions());
            pulsarTopicName = GroupMetadataManager.getTopicPartitionName(kafkaConfig.getKafkaMetadataTenant() + "/"
                    + kafkaConfig.getKafkaMetadataNamespace()
                    + "/" + Topic.GROUP_METADATA_TOPIC_NAME, partition);
        } else {
            throw new NotImplementedException("FindCoordinatorRequest not support TRANSACTION type " + type);
        }

        return findBroker(TopicName.get(pulsarTopicName));
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

        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP, JoinGroupRequest.class, JoinGroupRequest::groupId,
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

        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP, SyncGroupRequest.class, SyncGroupRequest::groupId,
                (SyncGroupRequest request) -> {
                    return  new SyncGroupResponse(
                            Errors.BROKER_NOT_AVAILABLE,
                            null
                    );
                });
    }

    protected void handleHeartbeatRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                          CompletableFuture<AbstractResponse> resultFuture) {

        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP, HeartbeatRequest.class, HeartbeatRequest::groupId,
                (HeartbeatRequest request) -> {
                    return  new HeartbeatResponse(0,
                            Errors.BROKER_NOT_AVAILABLE
                    );
                });
    }

    @Override
    protected void handleLeaveGroupRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                           CompletableFuture<AbstractResponse> resultFuture) {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP, LeaveGroupRequest.class, LeaveGroupRequest::groupId,
                (LeaveGroupRequest request) -> {
                    return new LeaveGroupResponse(Errors.BROKER_NOT_AVAILABLE);
                });
    }

    @Override
    protected void handleDescribeGroupRequest(KafkaHeaderAndRequest describeGroup,
                                              CompletableFuture<AbstractResponse> resultFuture) {
        resultFuture.completeExceptionally(new UnsupportedOperationException("not a proxy operation"));
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
        request.groups().forEach(k ->{
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

        throw new UnsupportedOperationException();
    }

    protected void handleDescribeConfigs(KafkaHeaderAndRequest describeConfigs,
                                         CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(describeConfigs.getRequest() instanceof DescribeConfigsRequest);
        DescribeConfigsRequest request = (DescribeConfigsRequest) describeConfigs.getRequest();

        throw new UnsupportedOperationException();
    }

    @Override
    protected void handleDeleteTopics(KafkaHeaderAndRequest deleteTopics,
                                      CompletableFuture<AbstractResponse> resultFuture) {
        checkArgument(deleteTopics.getRequest() instanceof DeleteTopicsRequest);
        DeleteTopicsRequest request = (DeleteTopicsRequest) deleteTopics.getRequest();
        Set<String> topicsToDelete = request.topics();
        throw new UnsupportedOperationException();
    }


    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Caught error in handler, closing channel", cause);
        ctx.close();
    }

    private CompletableFuture<Optional<String>>  getProtocolDataToAdvertise(String pulsarAddress,
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
        try {
            String principal;
            if (authenticator != null && authenticator.session() != null && authenticator.session().getPrincipal() != null) {
                principal = authenticator.session().getPrincipal().getName();
            } else {
                principal = null;
            }
            return CompletableFuture.completedFuture(admin.getAdminForPrincipal(principal));
        } catch (PulsarClientException err) {
            return FutureUtil.failedFuture(err);
        }
    }

    public CompletableFuture<PartitionMetadata> findBroker(TopicName topic) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Handle Lookup for {}", ctx.channel(), topic);
        }
        CompletableFuture<PartitionMetadata> returnFuture = new CompletableFuture<>();

        Node cached = topicsLeaders.get(topic.toString());
        if (cached != null) {
            returnFuture.complete(newPartitionMetadata(topic, cached));
            return returnFuture;
        }
        getPulsarAdmin().thenCompose(admin -> admin.lookups().lookupTopicAsync(topic.toString())
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

                if (log.isDebugEnabled()) {
                    log.debug("Found broker localListeners: {} for topicName: {}, "
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
        if (log.isDebugEnabled()) {
            log.debug("Return Broker Node of {}. {}:{}", address, address.getHostString(), address.getPort());
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

        if (log.isDebugEnabled()) {
            log.debug("Return PartitionMetadata node: {}, topicName: {}", node, topicName);
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

    // whether a ServiceLookupData contains wanted address.
    static boolean lookupDataContainsAddress(ServiceLookupData data, String hostAndPort) {
        return (data.getPulsarServiceUrl() != null && data.getPulsarServiceUrl().contains(hostAndPort))
            || (data.getPulsarServiceUrlTls() != null && data.getPulsarServiceUrlTls().contains(hostAndPort))
            || (data.getWebServiceUrl() != null && data.getWebServiceUrl().contains(hostAndPort))
            || (data.getWebServiceUrlTls() != null && data.getWebServiceUrlTls().contains(hostAndPort));
    }

    private static MemoryRecords validateRecords(short version, TopicPartition topicPartition, MemoryRecords records) {
        if (version >= 3) {
            Iterator<MutableRecordBatch> iterator = records.batches().iterator();
            if (!iterator.hasNext()) {
                throw new InvalidRecordException("Produce requests with version " + version + " must have at least "
                    + "one record batch");
            }

            MutableRecordBatch entry = iterator.next();
            if (entry.magic() != RecordBatch.MAGIC_VALUE_V2) {
                throw new InvalidRecordException("Produce requests with version " + version + " are only allowed to "
                    + "contain record batches with magic version 2");
            }

            if (iterator.hasNext()) {
                throw new InvalidRecordException("Produce requests with version " + version + " are only allowed to "
                    + "contain exactly one record batch");
            }
        }

        int validBytesCount = 0;
        for (RecordBatch batch : records.batches()) {
            if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 && batch.baseOffset() != 0) {
                throw new InvalidRecordException("The baseOffset of the record batch in the append to "
                    + topicPartition + " should be 0, but it is " + batch.baseOffset());
            }

            batch.ensureValid();
            validBytesCount += batch.sizeInBytes();
        }

        if (validBytesCount < 0) {
            throw new CorruptRecordException("Cannot append record batch with illegal length "
                + validBytesCount + " to log for " + topicPartition
                + ". A possible cause is corrupted produce request.");
        }

        MemoryRecords validRecords;
        if (validBytesCount == records.sizeInBytes()) {
            validRecords = records;
        } else {
            ByteBuffer validByteBuffer = records.buffer().duplicate();
            validByteBuffer.limit(validBytesCount);
            validRecords = MemoryRecords.readableRecords(validByteBuffer);
        }

        return validRecords;
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
    protected void handleListOffsetRequest(KafkaHeaderAndRequest listOffset, CompletableFuture<AbstractResponse> resultFuture)
    {
        checkArgument(listOffset.getRequest() instanceof ListOffsetRequest);
        ListOffsetRequest request = (ListOffsetRequest) listOffset.getRequest();

        log.info("handleListOffsetRequest {}", request.partitionTimestamps().entrySet());
        Map.Entry<TopicPartition, Long> first = request.partitionTimestamps().entrySet().iterator().next();
        final String fullPartitionName = KopTopic.toString(first.getKey());

        findBroker(TopicName.get(fullPartitionName))
                .thenAccept(brokerAddress -> {
                    grabConnectionToBroker(brokerAddress.leader().host(), brokerAddress.leader().port())
                            .forwardRequest(listOffset)
                            .thenAccept(response -> {
                                log.info("Forwarding ListOffSet response {}", response);
                                resultFuture.complete(response);
                            }).exceptionally(err -> {
                                AbstractResponse response = new ListOffsetResponse(new HashMap<>());
                                resultFuture.complete(response);
                                return null;
                            });
                }).exceptionally(err -> {
                    AbstractResponse response = new ListOffsetResponse(new HashMap<>());
                    resultFuture.complete(response);
                    return null;
                });
    }

    @Override
    protected void handleOffsetFetchRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> resultFuture)
    {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP, OffsetFetchRequest.class, OffsetFetchRequest::groupId,
                (OffsetFetchRequest request) -> {
                    return  new OffsetFetchResponse(Errors.BROKER_NOT_AVAILABLE, new HashMap<>());
                });
    }

    @Override
    protected void handleOffsetCommitRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> resultFuture)
    {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP, OffsetCommitRequest.class, OffsetCommitRequest::groupId,
                (OffsetCommitRequest request) -> {
                    Map<TopicPartition, Errors> map = request.offsetData().keySet().stream()
                            .collect(Collectors.toMap(Function.identity(), i -> Errors.BROKER_NOT_AVAILABLE));
                    return new OffsetCommitResponse(0, map);
                });
    }

   private <RT extends AbstractRequest, RST extends AbstractResponse> void handleRequestWithCoordinator(
           KafkaHeaderAndRequest kafkaHeaderAndRequest,
           CompletableFuture<AbstractResponse> resultFuture,
           FindCoordinatorRequest.CoordinatorType coordinatorType,
           Class<RT> requestClass,
           Function<RT, String> keyExtractor,
           Function<RT, RST> errorBuilder
           )
    {
        try
        {
            checkArgument(requestClass.isInstance(kafkaHeaderAndRequest.getRequest()));
            RT request = (RT) kafkaHeaderAndRequest.getRequest();

            String transactionalId = keyExtractor.apply(request);
            if (!isNoisyRequest(request)) {
                log.info("handleRequestWithCoordinator {} {} {} {}", request.getClass().getSimpleName(), request, transactionalId);
            }

            findCoordinator(coordinatorType, transactionalId)
                    .thenAccept(metadata -> {
                        grabConnectionToBroker(metadata.leader().host(), metadata.leader().port())
                                .forwardRequest(kafkaHeaderAndRequest)
                                .thenAccept(serverResponse -> {
                                    if (!isNoisyRequest(request)) {
                                        log.info("Sending {}.", serverResponse);
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
            log.error("Runtime error "+err, err);
            resultFuture.completeExceptionally(err);
        }
    }

    private <RT extends AbstractRequest> boolean isNoisyRequest(RT request) {
        // Consumers send these packets very often
        return (request instanceof HeartbeatRequest)
                || (request instanceof OffsetCommitRequest);
    }

    @Override
    protected void handleInitProducerId(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> resultFuture)
    {

        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.TRANSACTION, InitProducerIdRequest.class, InitProducerIdRequest::transactionalId,
                (InitProducerIdRequest request) -> {
                    return new InitProducerIdResponse(0, Errors.BROKER_NOT_AVAILABLE);
                });
    }

    @Override
    protected void handleAddPartitionsToTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> resultFuture)
    {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.TRANSACTION, AddPartitionsToTxnRequest.class, AddPartitionsToTxnRequest::transactionalId,
                (AddPartitionsToTxnRequest request) -> {
                    Map<TopicPartition, Errors> map = request.partitions().stream()
                            .collect(Collectors.toMap(Function.identity(), i -> Errors.BROKER_NOT_AVAILABLE));
                    return new AddPartitionsToTxnResponse(0, map);
                });
    }

    @Override
    protected void handleAddOffsetsToTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> resultFuture)
    {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.TRANSACTION, AddOffsetsToTxnRequest.class, AddOffsetsToTxnRequest::transactionalId,
                (AddOffsetsToTxnRequest request) -> {
                    // TODO: verify contents of the Map
                    return new AddPartitionsToTxnResponse(0, new HashMap<>());
                });

    }

    @Override
    protected void handleTxnOffsetCommit(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> resultFuture)
    {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.GROUP, TxnOffsetCommitRequest.class, TxnOffsetCommitRequest::consumerGroupId,
                (TxnOffsetCommitRequest request) -> {
                    // TODO: verify contents of the Map
                    return new TxnOffsetCommitResponse(0, new HashMap<>());
                });
    }

    @Override
    protected void handleEndTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> resultFuture)
    {
        handleRequestWithCoordinator(kafkaHeaderAndRequest, resultFuture, FindCoordinatorRequest.CoordinatorType.TRANSACTION, EndTxnRequest.class, EndTxnRequest::transactionalId,
                (EndTxnRequest request) -> {
                    return new EndTxnResponse(0, Errors.BROKER_NOT_AVAILABLE);
                });
    }

    @Override
    protected void handleWriteTxnMarkers(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> resultFuture)
    {
        resultFuture.completeExceptionally(new UnsupportedOperationException("not a proxy operation"));
    }

    private ConcurrentHashMap<String, ConnectionToBroker> connectionsToBrokers = new ConcurrentHashMap<>();

    private ConnectionToBroker grabConnectionToBroker(String brokerHost, int brokerPort) {
        String connectionKey = brokerHost+":"+brokerPort;
        return connectionsToBrokers.computeIfAbsent(connectionKey, (brokerAddress) -> {
            return new ConnectionToBroker(connectionKey, brokerHost, brokerPort);
        });
    }

    @AllArgsConstructor
    private static final class PendingAction {
        CompletableFuture<AbstractResponse> response;
        ApiKeys apiKeys;
        short apiVersion;
    }

    private class ConnectionToBroker implements Runnable {
        final String connectionKey;
        final String brokerHost;
        final int brokerPort;
        private DataOutputStream out;
        private InputStream in;
        private Socket socket;
        private Thread inputHandler;
        private volatile boolean closed;
        private CompletableFuture<?> connectionFuture;

        private final ConcurrentHashMap<Integer, PendingAction> pendingRequests = new ConcurrentHashMap<>();

        ConnectionToBroker(String connectionKey, String brokerHost, int brokerPort) {
            this.connectionKey = connectionKey;
            this.brokerHost = brokerHost;
            this.brokerPort = brokerPort;
        }

        public void run() {
            try
            {
                DataInputStream dataIn = new DataInputStream(in);
                while (true)
                {
                    int len = dataIn.readInt();
                    //log.info("read packet len {}", len);
                    byte[] buffer = new byte[len];
                    dataIn.readFully(buffer);
                    ByteBuffer responseBuffer = ByteBuffer.wrap(buffer, 0, len);
                    //log.info("packet {}", new String(buffer, 0, len, StandardCharsets.US_ASCII));
                    ResponseHeader header = ResponseHeader.parse(responseBuffer);
                    // Always expect the response version id to be the same as the request version id
                    //log.info("answer for {}", header.correlationId());

                    PendingAction result = pendingRequests.remove(header.correlationId());
                    if (result != null) {
                        Struct responseBody = result.apiKeys.parseResponse(result.apiVersion, responseBuffer);
                        AbstractResponse response = AbstractResponse.parseResponse(result.apiKeys, responseBody);
                        result.response.complete(response);
                    } else {
                        log.info("correlationId {} is unknown", header.correlationId());
                        throw new Exception("unknown correlationId " + header.correlationId());
                    }
                }
            } catch (SocketException t) {
                if (!closed) {
                    log.error("Network error {}", t);
                    close();
                }
            }catch (Throwable t) {
                log.error("bad error", t);
                close();
            }
        }

        private synchronized CompletableFuture<?> ensureConnection() {
            try
            {
                if (connectionFuture != null) {
                    return connectionFuture;
                }
                log.info("Opening proxy connection to {} {}", brokerHost, brokerPort);
                socket = new Socket(brokerHost, brokerPort);
                in = socket.getInputStream();
                out = new DataOutputStream(socket.getOutputStream());
                inputHandler = new Thread(this,"client-"+KafkaProxyRequestHandler.this.id + "-" + connectionKey);
                inputHandler.start();
                if (authenticator != null && authenticator.session() != null) {
                    String originalPrincipal = authenticator.session().getPrincipal().getName();
                    log.debug("Authenticating to KOP broker {} with {} identity", brokerHost + ":" + brokerPort, originalPrincipal);
                    connectionFuture = saslHandshake() // send SASL mechanism
                            .thenCompose( ___ -> authenticate()); // send Proxy Token, as Username we send the authenticated principal
                } else {
                    connectionFuture = CompletableFuture.completedFuture(socket);
                }
            } catch (Exception err) {
                log.error("cannot connect to {}", brokerHost + ":" + brokerPort + ":" + err);
                connectionsToBrokers.remove(connectionKey);
                connectionFuture = FutureUtil.failedFuture(err);
            }
            return connectionFuture;
        }

        private CompletableFuture<?> saslHandshake() {
            int dummyCorrelationId = getDummyCorrelationId();
            RequestHeader header = new RequestHeader(
                    ApiKeys.SASL_HANDSHAKE,
                    ApiKeys.SASL_HANDSHAKE.latestVersion(),
                    "proxy", //ignored
                    dummyCorrelationId
            );
            SaslHandshakeRequest  request = new SaslHandshakeRequest
                    .Builder("PLAIN")
                    .build();

            ByteBuffer buffer = request.serialize(header);

            KafkaHeaderAndRequest fullRequest = new KafkaHeaderAndRequest(
                    header,
                    request,
                    Unpooled.wrappedBuffer(buffer),
                    null
            );
            CompletableFuture<AbstractResponse> result = new CompletableFuture<>();
            sendRequestOnTheWire(fullRequest,result);
            return result.thenAccept(response -> {
                log.debug("SASL Handshake completed with success");
            });
        }

        private CompletableFuture<?> authenticate() {
            int dummyCorrelationId = getDummyCorrelationId();
            RequestHeader header = new RequestHeader(
                    ApiKeys.SASL_AUTHENTICATE,
                    ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                    "proxy", // ignored
                    dummyCorrelationId
            );

            String actualAuthenticationToken;
            try {
                // this can be token: or file://....
                actualAuthenticationToken = authenticationToken.getAuthData().getCommandData();
            } catch (PulsarClientException err) {
                log.info("Cannot read token for Proxy authentication", err);
                return FutureUtil.failedFuture(err);
            }
            if (actualAuthenticationToken == null) {
                log.info("This proxy has not been configuration for token authentication");
                return FutureUtil.failedFuture(new Exception("This proxy has not been configuration for token authentication"));
            }

            String originalPrincipal = authenticator.session().getPrincipal().getName();
            String prefix = "PROXY"; // the prefix PROXY means nothing, it is ignored by SaslUtils#parseSaslAuthBytes
            String password = "token:" + actualAuthenticationToken;
            String usernamePassword = prefix +
                    "\u0000" + originalPrincipal +
                    "\u0000" + password;
            byte[] saslAuthBytes = usernamePassword.getBytes(UTF_8);
            SaslAuthenticateRequest  request = new SaslAuthenticateRequest
                    .Builder(ByteBuffer.wrap(saslAuthBytes))
                    .build();

            ByteBuffer buffer = request.serialize(header);

            KafkaHeaderAndRequest fullRequest = new KafkaHeaderAndRequest(
                    header,
                    request,
                    Unpooled.wrappedBuffer(buffer),
                    null
            );
            CompletableFuture<AbstractResponse> result = new CompletableFuture<>();
            sendRequestOnTheWire(fullRequest,result);
            return result.thenAccept(response -> {
                    SaslAuthenticateResponse saslResponse = (SaslAuthenticateResponse) response;
                    if (saslResponse.error() != Errors.NONE) {
                        log.error("Failed authentication against KOP broker {}{}", saslResponse.error(), saslResponse.errorMessage());
                        close();
                        throw new CompletionException(saslResponse.error().exception());
                    } else {
                        log.debug("Success step AUTH to KOP broker {} {} {}", saslResponse.error(), saslResponse.errorMessage(), saslResponse.saslAuthBytes());
                    }
            });
        }

        public CompletableFuture<AbstractResponse> forwardRequest(KafkaHeaderAndRequest request) {
            CompletableFuture<AbstractResponse> result = new CompletableFuture<>();
            ensureConnection().whenComplete( (a, error) -> {
                if (error != null) {
                    result.completeExceptionally(error);
                    return;
                }
                sendRequestOnTheWire(request, result);
            });
            return result;
        }

        private void sendRequestOnTheWire(KafkaHeaderAndRequest request, CompletableFuture<AbstractResponse> result) {
            byte[] bytes = ByteBufUtil.getBytes(request.getBuffer());
            // the Kafka client sends unique values for this correlationId
            int correlationId = request.getHeader().correlationId();
            log.debug("Sending request id {} {} apiVersion {}", correlationId, new String(bytes, StandardCharsets.US_ASCII), request.getHeader().apiVersion());
            pendingRequests.put(correlationId, new PendingAction(result, request.getHeader().apiKey(), request.getHeader().apiVersion()));
            synchronized (out)
            {
                try
                {
                    out.writeInt(bytes.length);
                    out.write(bytes);
                    out.flush();
                }
                catch (IOException err)
                {
                    forgetMetadataForFailedBroker(brokerHost, brokerPort);
                    throw new CompletionException(err);
                }
            }
        }

        public void close() {
            closed = true;
            connectionsToBrokers.remove(connectionKey);
            pendingRequests.values().forEach(r->{
                r.response.completeExceptionally(new Exception("Connection closed by the client"));
            });
            if (socket != null)
            {
                try
                {
                    socket.close();
                } catch (Exception err) {
                    //
                }
            }
        }
    }

    private void forgetMetadataForFailedBroker(String brokerHost, int brokerPort) {
        Collection<String> keysToRemove  = topicsLeaders
                .entrySet()
                .stream()
                .filter(n -> n.getValue().port() == brokerPort && n.getValue().host().equals(brokerHost))
                .map(e->e.getKey())
                .collect(Collectors.toSet());
        log.info("forgetMetadataForFailedBroker {}:{} -> {}", brokerHost, brokerPort, keysToRemove);
        keysToRemove.forEach(topicsLeaders::remove);
    }
}
