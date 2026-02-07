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

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.KopResponseUtils;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.testng.annotations.Test;

public class KafkaCommandDecoderNullResponseTest {

    private static class NullResponseCommandDecoder extends KafkaCommandDecoder {
        NullResponseCommandDecoder(RequestStats requestStats,
                                   KafkaServiceConfiguration kafkaConfig) {
            super(requestStats, kafkaConfig, null);
        }

        @Override
        protected boolean hasAuthenticated() {
            return true;
        }

        @Override
        protected void channelPrepare(ChannelHandlerContext ctx,
                                      ByteBuf requestBuf,
                                      BiConsumer<Long, Throwable> registerRequestParseLatency,
                                      BiConsumer<ApiKeys, Long> registerRequestLatency)
                throws AuthenticationException {
            // no-op
        }

        @Override
        protected void maybeDelayCloseOnAuthenticationFailure() {
            // no-op
        }

        @Override
        protected void completeCloseOnAuthenticationFailure() {
            // no-op
        }

        @Override
        protected void handleInactive(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                      CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }

        @Override
        protected void handleApiVersionsRequest(KafkaHeaderAndRequest apiVersion,
                                                CompletableFuture<AbstractResponse> response) {
            response.complete(null);
        }

        @Override
        protected void handleTopicMetadataRequest(KafkaHeaderAndRequest metadata,
                                                  CompletableFuture<AbstractResponse> response) {
            handleError(metadata, response);
        }

        @Override
        protected void handleProduceRequest(KafkaHeaderAndRequest produce,
                                            CompletableFuture<AbstractResponse> response) {
            response.complete(null);
        }

        @Override
        protected void handleFindCoordinatorRequest(KafkaHeaderAndRequest findCoordinator,
                                                    CompletableFuture<AbstractResponse> response) {
            handleError(findCoordinator, response);
        }

        @Override
        protected void handleListOffsetRequest(KafkaHeaderAndRequest listOffset,
                                               CompletableFuture<AbstractResponse> response) {
            handleError(listOffset, response);
        }

        @Override
        protected void handleOffsetFetchRequest(KafkaHeaderAndRequest offsetFetch,
                                                CompletableFuture<AbstractResponse> response) {
            handleError(offsetFetch, response);
        }

        @Override
        protected void handleOffsetCommitRequest(KafkaHeaderAndRequest offsetCommit,
                                                 CompletableFuture<AbstractResponse> response) {
            handleError(offsetCommit, response);
        }

        @Override
        protected void handleFetchRequest(KafkaHeaderAndRequest fetch,
                                          CompletableFuture<AbstractResponse> response) {
            handleError(fetch, response);
        }

        @Override
        protected void handleJoinGroupRequest(KafkaHeaderAndRequest joinGroup,
                                              CompletableFuture<AbstractResponse> response) {
            handleError(joinGroup, response);
        }

        @Override
        protected void handleSyncGroupRequest(KafkaHeaderAndRequest syncGroup,
                                              CompletableFuture<AbstractResponse> response) {
            handleError(syncGroup, response);
        }

        @Override
        protected void handleHeartbeatRequest(KafkaHeaderAndRequest heartbeat,
                                              CompletableFuture<AbstractResponse> response) {
            handleError(heartbeat, response);
        }

        @Override
        protected void handleLeaveGroupRequest(KafkaHeaderAndRequest leaveGroup,
                                               CompletableFuture<AbstractResponse> response) {
            handleError(leaveGroup, response);
        }

        @Override
        protected void handleDescribeGroupRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                                  CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }

        @Override
        protected void handleDescribeProducersRequest(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                                      CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }

        @Override
        protected void handleListGroupsRequest(KafkaHeaderAndRequest listGroups,
                                               CompletableFuture<AbstractResponse> response) {
            handleError(listGroups, response);
        }

        @Override
        protected void handleListTransactionsRequest(KafkaHeaderAndRequest listGroups,
                                                     CompletableFuture<AbstractResponse> response) {
            handleError(listGroups, response);
        }

        @Override
        protected void handleDescribeTransactionsRequest(KafkaHeaderAndRequest listGroups,
                                                         CompletableFuture<AbstractResponse> response) {
            handleError(listGroups, response);
        }

        @Override
        protected void handleDeleteGroupsRequest(KafkaHeaderAndRequest deleteGroups,
                                                 CompletableFuture<AbstractResponse> response) {
            handleError(deleteGroups, response);
        }

        @Override
        protected void handleSaslAuthenticate(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                              CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }

        @Override
        protected void handleSaslHandshake(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                           CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }

        @Override
        protected void handleCreateTopics(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                          CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }

        @Override
        protected void handleDescribeConfigs(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                             CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }

        @Override
        protected void handleAlterConfigs(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                          CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }

        @Override
        protected void handleInitProducerId(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                            CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }

        @Override
        protected void handleAddPartitionsToTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                                CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }

        @Override
        protected void handleAddOffsetsToTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                             CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }

        @Override
        protected void handleTxnOffsetCommit(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                             CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }

        @Override
        protected void handleEndTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                    CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }

        @Override
        protected void handleWriteTxnMarkers(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                             CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }

        @Override
        protected void handleDeleteTopics(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                          CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }

        @Override
        protected void handleDeleteRecords(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                           CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }

        @Override
        protected void handleCreatePartitions(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                              CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }

        @Override
        protected void handleDescribeCluster(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                             CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }

        @Override
        protected void handleDescribeClientQuotas(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                                  CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }

        @Override
        protected void handleAlterClientQuotas(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                               CompletableFuture<AbstractResponse> response) {
            handleError(kafkaHeaderAndRequest, response);
        }
    }

    @Test
    public void testNullResponseNonProduceSendsErrorResponse() {
        KafkaServiceConfiguration config = new KafkaServiceConfiguration();
        NullResponseCommandDecoder decoder = new NullResponseCommandDecoder(RequestStats.NULL_INSTANCE, config);

        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        try {
            channel.pipeline().fireChannelActive();

            RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, "c1", 1);
            ApiVersionsRequest request = new ApiVersionsRequest.Builder().build();
            ByteBuf requestBuf = KopResponseUtils.serializeRequest(header, request);

            channel.writeInbound(requestBuf);
            channel.runPendingTasks();
            channel.runScheduledPendingTasks();

            ByteBuf responseBuf = channel.readOutbound();
            assertNotNull(responseBuf, "Expected an error response for unexpected null response");
            ReferenceCountUtil.safeRelease(responseBuf);

            assertTrue(requestBuf.refCnt() == 0, "Request buffer should be released");
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testNullResponseProduceAcks0IsDropped() {
        KafkaServiceConfiguration config = new KafkaServiceConfiguration();
        NullResponseCommandDecoder decoder = new NullResponseCommandDecoder(RequestStats.NULL_INSTANCE, config);

        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        try {
            channel.pipeline().fireChannelActive();

            short produceVersion = ApiKeys.PRODUCE.latestVersion();
            ProduceRequestData data = new ProduceRequestData()
                    .setAcks((short) 0)
                    .setTimeoutMs(0);
            ProduceRequest request = new ProduceRequest(data, produceVersion);
            RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, produceVersion, "c1", 1);
            ByteBuf requestBuf = KopResponseUtils.serializeRequest(header, request);

            channel.writeInbound(requestBuf);
            channel.runPendingTasks();
            channel.runScheduledPendingTasks();

            ByteBuf responseBuf = channel.readOutbound();
            assertNull(responseBuf, "Expected no response for Produce acks=0");
            assertTrue(requestBuf.refCnt() == 0, "Request buffer should be released");
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}
