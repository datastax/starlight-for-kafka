package io.streamnative.pulsar.handlers.kop;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.streamnative.pulsar.handlers.kop.schemaregistry.HttpRequestProcessor;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryRequestAuthenticator;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.impl.SchemaStorageException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Response;

@Slf4j
public class ProxySchemaRegistryHttpRequestProcessor extends HttpRequestProcessor {

    private final AsyncHttpClient client;
    private final Supplier<String> brokerUrlSupplier;
    private final SchemaRegistryRequestAuthenticator schemaRegistryRequestAuthenticator;
    private final KafkaServiceConfiguration kafkaServiceConfiguration;

    public ProxySchemaRegistryHttpRequestProcessor(Supplier<String> brokerUrlSupplier,
                                                   KafkaServiceConfiguration kafkaServiceConfiguration,
                                                   SchemaRegistryRequestAuthenticator schemaRegistryRequestAuthenticator
                                                   ) {
        this.brokerUrlSupplier = brokerUrlSupplier;
        this.kafkaServiceConfiguration = kafkaServiceConfiguration;
        this.schemaRegistryRequestAuthenticator = schemaRegistryRequestAuthenticator;
        this.client = Dsl.asyncHttpClient(Dsl
                .config()
                .setConnectTimeout(5000)
                .setFollowRedirect(false)
                .setKeepAlive(false)
                .setUseInsecureTrustManager(true)
                .setUserAgent("KOPProxy"));
    }

    @Override
    public void close() {
        try {
            this.client.close();
        } catch  (IOException err) {
            log.error("Error closing HTTP Client", err);
        }
    }

    @Override
    protected boolean acceptRequest(FullHttpRequest request) {
        String uri = request.uri();
        return uri.startsWith("/schemas")
                || uri.startsWith("/subjects");
    }

    protected CompletableFuture<FullHttpResponse> processRequest(FullHttpRequest request) {
        if (kafkaServiceConfiguration.isAuthenticationEnabled()) {
            try {
                String currentTenant = schemaRegistryRequestAuthenticator.authenticate(request);
                if (currentTenant == null) {
                    throw new SchemaStorageException("Missing or failed authentication",
                            HttpResponseStatus.UNAUTHORIZED.code());
                }
            } catch (SchemaStorageException err) {
                return CompletableFuture.completedFuture(buildJsonErrorResponse(err));
            }
        }

        String uri = request.uri();
        String brokerUrl = brokerUrlSupplier.get();
        String fullUrl = brokerUrl + uri;

        BoundRequestBuilder preparedRequest = client.prepare(request.method().name(), fullUrl);
        request.headers().forEach((header) -> {
            preparedRequest.addHeader(header.getKey(), header.getValue());
        });
        if (request.method().equals(HttpMethod.POST)
                || request.method().equals(HttpMethod.PUT)) {
            preparedRequest.setBody(ByteBufUtil.getBytes(request.content()));
        }
        return preparedRequest
                .execute()
                .toCompletableFuture()
                .thenApply((Response response) -> {
                    DefaultHttpHeaders respHeaders = new DefaultHttpHeaders();
                    respHeaders.add(response.getHeaders());
                    FullHttpResponse res = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                            HttpResponseStatus.valueOf(response.getStatusCode(), response.getStatusText()),
                            Unpooled.wrappedBuffer(response.getResponseBodyAsBytes()),
                            respHeaders, new DefaultHttpHeaders());
                    return res;
                }).exceptionally(err -> {
                    return buildJsonErrorResponse(new SchemaStorageException(err));
                });
    }
}
