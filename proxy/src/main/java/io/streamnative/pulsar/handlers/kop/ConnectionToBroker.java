package io.streamnative.pulsar.handlers.kop;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.*;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.util.FutureUtil;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
class ConnectionToBroker implements Runnable {
    private final KafkaProxyRequestHandler kafkaProxyRequestHandler;
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

    ConnectionToBroker(KafkaProxyRequestHandler kafkaProxyRequestHandler, String connectionKey, String brokerHost, int brokerPort) {
        this.kafkaProxyRequestHandler = kafkaProxyRequestHandler;
        this.connectionKey = connectionKey;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
    }

    public void run() {
        try {
            DataInputStream dataIn = new DataInputStream(in);
            while (true) {
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
        } catch (Throwable t) {
            log.error("bad error", t);
            close();
        }
    }

    private synchronized CompletableFuture<?> ensureConnection() {
        try {
            if (connectionFuture != null) {
                return connectionFuture;
            }
            log.info("Opening proxy connection to {} {}", brokerHost, brokerPort);
            socket = new Socket(brokerHost, brokerPort);
            in = socket.getInputStream();
            out = new DataOutputStream(socket.getOutputStream());
            inputHandler = new Thread(this, "client-" + kafkaProxyRequestHandler.id + "-" + connectionKey);
            inputHandler.start();
            String originalPrincipal = kafkaProxyRequestHandler.currentUser();
            if (originalPrincipal != null) {
                log.debug("Authenticating to KOP broker {} with {} identity", brokerHost + ":" + brokerPort, originalPrincipal);
                connectionFuture = saslHandshake() // send SASL mechanism
                        .thenCompose(___ -> authenticate()); // send Proxy Token, as Username we send the authenticated principal
            } else {
                connectionFuture = CompletableFuture.completedFuture(socket);
            }
        } catch (Exception err) {
            log.error("cannot connect to {}", brokerHost + ":" + brokerPort + ":" + err);
            kafkaProxyRequestHandler.forgetMetadataForFailedBroker(brokerHost, brokerPort);
            kafkaProxyRequestHandler.discardConnectionToBroker(this);
            connectionFuture = FutureUtil.failedFuture(err);
        }
        return connectionFuture;
    }

    private CompletableFuture<?> saslHandshake() {
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
        CompletableFuture<AbstractResponse> result = new CompletableFuture<>();
        sendRequestOnTheWire(fullRequest, result);
        return result.thenAccept(response -> {
            log.debug("SASL Handshake completed with success");
        });
    }

    private CompletableFuture<?> authenticate() {
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
            return FutureUtil.failedFuture(new Exception("This proxy has not been configuration for token authentication"));
        }

        String originalPrincipal = kafkaProxyRequestHandler.currentUser();
        String prefix = "PROXY"; // the prefix PROXY means nothing, it is ignored by SaslUtils#parseSaslAuthBytes
        String password = "token:" + actualAuthenticationToken;
        String usernamePassword = prefix +
                "\u0000" + originalPrincipal +
                "\u0000" + password;
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
        sendRequestOnTheWire(fullRequest, result);
        return result.thenAccept(response -> {
            SaslAuthenticateResponse saslResponse = (SaslAuthenticateResponse) response;
            if (saslResponse.error() != Errors.NONE) {
                kafkaProxyRequestHandler.forgetMetadataForFailedBroker(brokerHost, brokerPort);
                log.error("Failed authentication against KOP broker {}{}", saslResponse.error(), saslResponse.errorMessage());
                close();
                throw new CompletionException(saslResponse.error().exception());
            } else {
                log.debug("Success step AUTH to KOP broker {} {} {}", saslResponse.error(), saslResponse.errorMessage(), saslResponse.saslAuthBytes());
            }
        });
    }

    public CompletableFuture<AbstractResponse> forwardRequest(KafkaCommandDecoder.KafkaHeaderAndRequest request) {
        CompletableFuture<AbstractResponse> result = new CompletableFuture<>();
        ensureConnection().whenComplete((a, error) -> {
            if (error != null) {
                kafkaProxyRequestHandler.forgetMetadataForFailedBroker(brokerHost, brokerPort);
                result.completeExceptionally(error);
                return;
            }
            sendRequestOnTheWire(request, result);
        });
        return result;
    }

    private void sendRequestOnTheWire(KafkaCommandDecoder.KafkaHeaderAndRequest request, CompletableFuture<AbstractResponse> result) {
        byte[] bytes = ByteBufUtil.getBytes(request.getBuffer());
        // the Kafka client sends unique values for this correlationId
        int correlationId = request.getHeader().correlationId();
        log.debug("Sending request id {} {} apiVersion {}", correlationId, new String(bytes, StandardCharsets.US_ASCII), request.getHeader().apiVersion());
        pendingRequests.put(correlationId, new PendingAction(result, request.getHeader().apiKey(), request.getHeader().apiVersion()));
        synchronized (out) {
            try {
                out.writeInt(bytes.length);
                out.write(bytes);
                out.flush();
            } catch (IOException err) {
                kafkaProxyRequestHandler.forgetMetadataForFailedBroker(brokerHost, brokerPort);
                throw new CompletionException(err);
            }
        }
    }

    public void close() {
        closed = true;
        kafkaProxyRequestHandler.discardConnectionToBroker(this);
        pendingRequests.values().forEach(r -> {
            r.response.completeExceptionally(new Exception("Connection closed by the client"));
        });
        if (socket != null) {
            try {
                socket.close();
            } catch (Exception err) {
                //
            }
        }
    }


    @AllArgsConstructor
    private static final class PendingAction {
        CompletableFuture<AbstractResponse> response;
        ApiKeys apiKeys;
        short apiVersion;
    }
}
