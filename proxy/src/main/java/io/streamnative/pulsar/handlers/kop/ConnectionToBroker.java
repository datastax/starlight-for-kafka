package io.streamnative.pulsar.handlers.kop;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.util.FutureUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.*;

import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
class ConnectionToBroker {
    private final KafkaProxyRequestHandler kafkaProxyRequestHandler;
    final String connectionKey;
    final String brokerHost;
    final int brokerPort;
    private volatile boolean closed;
    private CompletableFuture<Channel> connectionFuture;
    private final BlockingQueue<Map.Entry<KafkaCommandDecoder.KafkaHeaderAndRequest,
            CompletableFuture<AbstractResponse>>> writeQueue = new LinkedBlockingQueue<>();

    private final ConcurrentHashMap<Integer, PendingAction> pendingRequests = new ConcurrentHashMap<>();

    ConnectionToBroker(KafkaProxyRequestHandler kafkaProxyRequestHandler, String connectionKey, String brokerHost,
                       int brokerPort) {
        this.kafkaProxyRequestHandler = kafkaProxyRequestHandler;
        this.connectionKey = connectionKey;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
    }

    private synchronized CompletableFuture<Channel> ensureConnection() {
        if (connectionFuture != null) {
            return connectionFuture;
        }
        log.info("Opening proxy connection to {} {}", brokerHost, brokerPort);

        Bootstrap b = new Bootstrap();
        b.group(kafkaProxyRequestHandler.getWorkerGroup());
        b.channel(NioSocketChannel.class);
        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
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
                    .thenCompose(this::authenticate); // send Proxy Token, as Username we send the authenticated principal
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
        int dummyCorrelationId = kafkaProxyRequestHandler.getDummyCorrelationId();
        RequestHeader header = new RequestHeader(
                ApiKeys.SASL_HANDSHAKE,
                ApiKeys.SASL_HANDSHAKE.latestVersion(),
                "proxy", //ignored
                dummyCorrelationId
        );
        SaslHandshakeRequest request = new SaslHandshakeRequest
                .Builder("PLAIN")
                .build();
        ByteBuffer buffer = request.serialize(header);
        KafkaCommandDecoder.KafkaHeaderAndRequest fullRequest = new KafkaCommandDecoder.KafkaHeaderAndRequest(
                header,
                request,
                Unpooled.wrappedBuffer(buffer),
                null
        );
        return fullRequest;
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

        String actualAuthenticationToken;
        try {
            // this can be token: or file://....
            actualAuthenticationToken = kafkaProxyRequestHandler.getClientToken();
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
        String prefix = "PROXY"; // the prefix PROXY means nothing, it is ignored by SaslUtils#parseSaslAuthBytes
        String password = "token:" + actualAuthenticationToken;
        String usernamePassword = prefix
                + "\u0000" + originalPrincipal
                + "\u0000" + password;
        byte[] saslAuthBytes = usernamePassword.getBytes(UTF_8);
        SaslAuthenticateRequest request = new SaslAuthenticateRequest
                .Builder(ByteBuffer.wrap(saslAuthBytes))
                .build();

        ByteBuffer buffer = request.serialize(header);

        KafkaCommandDecoder.KafkaHeaderAndRequest fullRequest = new KafkaCommandDecoder.KafkaHeaderAndRequest(
                header,
                request,
                Unpooled.wrappedBuffer(buffer),
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
            processWriteQueue(channel, error);

        });
        return result;
    }

    private void sendRequestOnTheWire(Channel channel, KafkaCommandDecoder.KafkaHeaderAndRequest request,
                                      CompletableFuture<AbstractResponse> result) {
        if (closed) {
            result.completeExceptionally(new IOException("connection closed"));
            return;
        }
        byte[] bytes = ByteBufUtil.getBytes(request.getBuffer());
        // the Kafka client sends unique values for this correlationId
        int correlationId = request.getHeader().correlationId();
        if (log.isDebugEnabled()) {
            log.debug("{} Sending request id {} apiVersion {} request {}", System.identityHashCode(this),
                    correlationId, request.getHeader().apiVersion(), request);
        }
        PendingAction existing = pendingRequests.put(correlationId, new PendingAction(result,
                request.getHeader().apiKey(), request.getHeader().apiVersion()));
        if (existing != null) {
            result.completeExceptionally(new IOException("correlationId " + correlationId + " already inflight"));
            return;
        }
        channel.writeAndFlush(Unpooled.wrappedBuffer(bytes)).addListener(writeFuture -> {
            if (!writeFuture.isSuccess()) {
                pendingRequests.remove(correlationId);
                kafkaProxyRequestHandler.forgetMetadataForFailedBroker(brokerHost, brokerPort);
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
            ResponseHeader header = ResponseHeader.parse(asBuffer);
            // Always expect the response version id to be the same as the request version id
            PendingAction result = pendingRequests.remove(header.correlationId());
            if (result != null) {
                Struct responseBody = result.apiKeys.parseResponse(result.apiVersion, asBuffer);
                AbstractResponse response = AbstractResponse.parseResponse(result.apiKeys, responseBody);
                // TODO, this probably should not happen in Netty Eventloop
                result.response.complete(response);
            } else {
                log.error("correlationId {} is unknown", header.correlationId());
                ctx.fireExceptionCaught(new Exception("Correlation ID " + header.correlationId() + " is unknown"));
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("Error on {}", connectionKey, cause);
            close();
        }
    }


    @AllArgsConstructor
    private static final class PendingAction {
        CompletableFuture<AbstractResponse> response;
        ApiKeys apiKeys;
        short apiVersion;
    }
}
