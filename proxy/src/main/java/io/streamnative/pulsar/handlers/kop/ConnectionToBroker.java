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
import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslHandler;
import io.streamnative.pulsar.handlers.kop.security.PlainSaslServer;
import io.streamnative.pulsar.handlers.kop.utils.ssl.SSLUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import javax.security.sasl.SaslException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.KopResponseUtils;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.eclipse.jetty.util.ssl.SslContextFactory;

@Slf4j
class ConnectionToBroker {
    final String connectionKey;
    final String brokerHost;
    final int brokerPort;
    private final KafkaProxyRequestHandler kafkaProxyRequestHandler;
    private final BlockingQueue<Map.Entry<KafkaCommandDecoder.KafkaHeaderAndRequest,
            CompletableFuture<AbstractResponse>>> writeQueue = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<Integer, PendingAction> pendingRequests = new ConcurrentHashMap<>();
    private volatile boolean closed;
    private CompletableFuture<Channel> connectionFuture;
    private final SslContextFactory.Client sslContextFactory;
    private boolean enableTls;

    ConnectionToBroker(KafkaProxyRequestHandler kafkaProxyRequestHandler, String connectionKey, String brokerHost,
                       int brokerPort) {
        this.kafkaProxyRequestHandler = kafkaProxyRequestHandler;
        this.connectionKey = connectionKey;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        KafkaServiceConfiguration kafkaConfig = kafkaProxyRequestHandler.getKafkaConfig();
        this.enableTls = kafkaConfig.isKopTlsEnabledWithBroker();
        if (this.enableTls) {
            sslContextFactory = SSLUtils.createClientSslContextFactory(kafkaConfig);
        } else {
            sslContextFactory = null;
        }
    }

    private synchronized CompletableFuture<Channel> ensureConnection() {
        if (connectionFuture != null) {
            return connectionFuture;
        }
        log.info("Opening proxy connection to {}:{} current user {}", brokerHost, brokerPort,
                kafkaProxyRequestHandler.currentUser());

        EventLoopGroup workerGroup = kafkaProxyRequestHandler.getWorkerGroup();
        Class<? extends SocketChannel> clientSocketChannelClass;
        if (workerGroup instanceof SingleThreadEventLoop) {
            // handle Epool
            clientSocketChannelClass =
                    EventLoopUtil.getClientSocketChannelClass(((SingleThreadEventLoop) workerGroup).parent());
        } else {
            clientSocketChannelClass = NioSocketChannel.class;
        }
        Bootstrap b = new Bootstrap();
        b.group(workerGroup);
        b.channel(clientSocketChannelClass);
        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                if (enableTls) {
                    ch.pipeline().addLast(TLS_HANDLER,
                            new SslHandler(SSLUtils.createClientSslEngine(sslContextFactory, brokerHost, brokerPort)));
                }
                ch.pipeline().addLast(new LengthFieldPrepender(4));
                ch.pipeline().addLast("frameDecoder",
                        new LengthFieldBasedFrameDecoder(KafkaProxyChannelInitializer.MAX_FRAME_LENGTH, 0, 4, 0, 4));
                ch.pipeline().addLast(new ResponseFromBrokerHandler());
            }
        });

        CompletableFuture<Channel> rawConnectFuture = new CompletableFuture<>();
        // Start the client.
        ChannelFuture f = b.connect(brokerHost, brokerPort); // (5)
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                if (channelFuture.isSuccess()) {
                    log.info("Connected to {}", connectionKey);
                    rawConnectFuture.complete(channelFuture.channel());
                } else {
                    log.error("Cannot connect to {}", connectionKey, channelFuture.cause());
                    kafkaProxyRequestHandler.forgetMetadataForFailedBroker(brokerHost, brokerPort);
                    kafkaProxyRequestHandler.discardConnectionToBroker(ConnectionToBroker.this);
                    rawConnectFuture.completeExceptionally(channelFuture.cause());
                }
            }
        });

        String originalPrincipal = kafkaProxyRequestHandler.currentUser();
        if (originalPrincipal != null) {
            log.debug("Authenticating to KOP broker {} with {} identity", brokerHost + ":" + brokerPort,
                    originalPrincipal);
            connectionFuture = rawConnectFuture
                    .thenCompose(this::saslHandshake) // send SASL mechanism
                    .thenCompose(
                            this::authenticate); // send Proxy Token, as Username we send the authenticated principal
        } else {
            connectionFuture = rawConnectFuture;
        }
        return connectionFuture;
    }

    private CompletableFuture<Channel> saslHandshake(Channel channel) {
        KafkaCommandDecoder.KafkaHeaderAndRequest fullRequest = buildSASLRequest();
        CompletableFuture<AbstractResponse> result = new CompletableFuture<>();
        sendRequestOnTheWire(channel, fullRequest, result);
        result.exceptionally(error -> {
            // ensure that we close the channel
            channel.close();
            return null;
        });
        return result.thenApply(response -> {
            log.debug("SASL Handshake completed with success");
            return channel;
        });
    }

    private KafkaCommandDecoder.KafkaHeaderAndRequest buildSASLRequest() {
        String mechanism = getMechanism();
        int dummyCorrelationId = kafkaProxyRequestHandler.getDummyCorrelationId();
        RequestHeader header = new RequestHeader(
                ApiKeys.SASL_HANDSHAKE,
                ApiKeys.SASL_HANDSHAKE.latestVersion(),
                "proxy", //ignored
                dummyCorrelationId
        );
        SaslHandshakeRequest request = new SaslHandshakeRequest
                .Builder(new SaslHandshakeRequestData()
                .setMechanism(mechanism))
                .build();
        ByteBuf buffer = KopResponseUtils.serializeRequest(header, request);
        KafkaCommandDecoder.KafkaHeaderAndRequest fullRequest = new KafkaCommandDecoder.KafkaHeaderAndRequest(
                header,
                request,
                buffer,
                null
        );
        return fullRequest;
    }

    private String getMechanism() {
        String mechanism;
        if (kafkaProxyRequestHandler.getAuthentication() instanceof AuthenticationOAuth2) {
            mechanism = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
        } else {
            mechanism = PlainSaslServer.PLAIN_MECHANISM;
        }
        return mechanism;
    }

    private CompletableFuture<Channel> authenticate(final Channel channel) {
        CompletableFuture<Channel> internal = authenticateInternal(channel);
        // ensure that we close the channel
        internal.exceptionally(error -> {
            channel.close();
            return null;
        });
        return internal;
    }

    private CompletableFuture<Channel> authenticateInternal(Channel channel) {
        int dummyCorrelationId = kafkaProxyRequestHandler.getDummyCorrelationId();
        RequestHeader header = new RequestHeader(
                ApiKeys.SASL_AUTHENTICATE,
                ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                "proxy", // ignored
                dummyCorrelationId
        );

        String mechanism = getMechanism();
        byte[] saslAuthBytes;

        switch (mechanism) {
            case PlainSaslServer.PLAIN_MECHANISM:
                String actualAuthenticationToken;
                try {
                    // this can be token: or file://....
                    actualAuthenticationToken = kafkaProxyRequestHandler
                            .getAuthentication().getAuthData().getCommandData();
                } catch (PulsarClientException err) {
                    log.info("Cannot read token for Proxy authentication", err);
                    return FutureUtil.failedFuture(err);
                }
                if (actualAuthenticationToken == null) {
                    log.info("This proxy has not been configuration for token authentication");
                    return FutureUtil.failedFuture(
                            new Exception("This proxy has not been configuration for token authentication"));
                }

                String originalPrincipal = kafkaProxyRequestHandler.currentUser();
                String originalTenant = kafkaProxyRequestHandler.getCurrentTenant();
                String username = originalTenant != null ? originalPrincipal + "/" + originalTenant : originalPrincipal;
                String prefix = "PROXY"; // the prefix PROXY means nothing, ignored by SaslUtils#parseSaslAuthBytes
                String password = "token:" + actualAuthenticationToken;
                String usernamePassword = prefix
                        + "\u0000" + username
                        + "\u0000" + password;
                saslAuthBytes = usernamePassword.getBytes(UTF_8);
                break;
            case OAuthBearerLoginModule.OAUTHBEARER_MECHANISM:
                try {
                    String commandData = kafkaProxyRequestHandler
                            .getAuthentication().getAuthData().getCommandData();
                    saslAuthBytes = new OAuthBearerClientInitialResponse(commandData, null, null)
                            .toBytes();
                } catch (SaslException | PulsarClientException oauthError) {
                    log.error("Error while preparing authentication", oauthError);
                    return FutureUtil.failedFuture(oauthError);
                }
                break;
            default:
                log.error("No corresponding mechanism to {}",
                        kafkaProxyRequestHandler.getAuthentication().getClass().getName());
                saslAuthBytes = new byte[0];
                break;
        }

        SaslAuthenticateRequest request = new SaslAuthenticateRequest
                .Builder(new SaslAuthenticateRequestData()
                .setAuthBytes(saslAuthBytes))
                .build();

        ByteBuf buffer = KopResponseUtils.serializeRequest(header, request);

        KafkaCommandDecoder.KafkaHeaderAndRequest fullRequest = new KafkaCommandDecoder.KafkaHeaderAndRequest(
                header,
                request,
                buffer,
                null
        );
        CompletableFuture<AbstractResponse> result = new CompletableFuture<>();
        sendRequestOnTheWire(channel, fullRequest, result);
        return result.thenApply(response -> {
            SaslAuthenticateResponse saslResponse = (SaslAuthenticateResponse) response;
            if (saslResponse.error() != Errors.NONE) {
                kafkaProxyRequestHandler.forgetMetadataForFailedBroker(brokerHost, brokerPort);
                log.error("Failed authentication against KOP broker {}{}", saslResponse.error(),
                        saslResponse.errorMessage());
                close();
                throw new CompletionException(saslResponse.error().exception());
            } else {
                log.debug("Success step AUTH to KOP broker {} {} {}", saslResponse.error(),
                        saslResponse.errorMessage(), saslResponse.saslAuthBytes());
            }
            return channel;
        });
    }

    private void processWriteQueue(Channel channel, Throwable error) {
        Map.Entry<KafkaCommandDecoder.KafkaHeaderAndRequest, CompletableFuture<AbstractResponse>> entry =
                writeQueue.poll();
        if (entry == null) {
            // this should not happen
            log.error("processWriteQueue failed");
            return;
        }
        KafkaCommandDecoder.KafkaHeaderAndRequest request = entry.getKey();
        CompletableFuture<AbstractResponse> result = entry.getValue();
        if (error != null) {
            log.error("error", error);
            kafkaProxyRequestHandler.forgetMetadataForFailedBroker(brokerHost, brokerPort);
            result.completeExceptionally(error);
            return;
        }
        sendRequestOnTheWire(channel, request, result);
    }

    public CompletableFuture<AbstractResponse> forwardRequest(KafkaCommandDecoder.KafkaHeaderAndRequest request) {
        CompletableFuture<AbstractResponse> result = new CompletableFuture<>();
        writeQueue.add(new AbstractMap.SimpleImmutableEntry<>(request, result));
        ensureConnection().whenComplete((channel, error) -> {
            // this execution may not process the request, but
            // the tip of the queue
            // CompletableFuture#whenComplete does not
            // provide ordering guarantees
            processWriteQueue(channel, error);
        });
        return result;
    }

    private void sendRequestOnTheWire(Channel channel, KafkaCommandDecoder.KafkaHeaderAndRequest request,
                                      CompletableFuture<AbstractResponse> result) {
        ByteBuf bytes = request.getBuffer();
        if (closed) {
            request.close();
            result.completeExceptionally(new IOException("connection closed"));
            return;
        }
        // the Kafka client sends unique values for this correlationId
        int correlationId = request.getHeader().correlationId();
        if (log.isDebugEnabled()) {
            log.debug("{} Sending request id {} apiVersion {} request {}", System.identityHashCode(this),
                    correlationId, request.getHeader().apiVersion(), request);
        }
        PendingAction existing = pendingRequests.put(correlationId, new PendingAction(result,
                request.getHeader()));
        if (existing != null) {
            request.close();
            result.completeExceptionally(new IOException("correlationId " + correlationId + " already inflight"));
            return;
        }
        channel.writeAndFlush(bytes).addListener(writeFuture -> {
            request.bufferReleased();
            if (!writeFuture.isSuccess()) {
                pendingRequests.remove(correlationId);
                kafkaProxyRequestHandler.forgetMetadataForFailedBroker(brokerHost, brokerPort);
                // cannot write, so we have to "close()" and trigger failure of every other
                // pending request and discard the reference to this connection
                close();
                result.completeExceptionally(writeFuture.cause());
            }
        });
    }

    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        kafkaProxyRequestHandler.discardConnectionToBroker(this);
        pendingRequests.values().forEach(r -> {
            r.response.completeExceptionally(new Exception("Connection closed by the client"));
        });
        if (connectionFuture != null) {
            connectionFuture.whenComplete((c, er) -> {
                if (er == null) {
                    c.close();
                }
            });
        }
    }

    @AllArgsConstructor
    private static final class PendingAction {
        CompletableFuture<AbstractResponse> response;
        RequestHeader requestHeader;
    }

    public class ResponseFromBrokerHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            byte[] buffer;
            // we should make a copy here, otherwise we could see bad effects
            // during the processing of the Response
            ByteBuf m = (ByteBuf) msg;
            try {
                buffer = ByteBufUtil.getBytes(m);
            } finally {
                m.release();
            }

            ByteBuffer asBuffer = ByteBuffer.wrap(buffer);
            int correlationId = asBuffer.getInt(0);
            // Always expect the response version id to be the same as the request version id
            PendingAction result = pendingRequests.remove(correlationId);
            if (result != null) {
                AbstractResponse response = AbstractResponse.parseResponse(
                        asBuffer, result.requestHeader);
                // TODO, this probably should not happen in Netty Eventloop
                result.response.complete(response);
            } else {
                log.error("correlationId {} is unknown", correlationId);
                ctx.fireExceptionCaught(new Exception("Correlation ID " + correlationId + " is unknown"));
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("Error on {}", connectionKey, cause);
            close();
        }
    }
}
