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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslHandler;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupCoordinator;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import io.streamnative.pulsar.handlers.kop.stats.StatsLogger;
import io.streamnative.pulsar.handlers.kop.utils.ssl.SSLUtils;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import static io.streamnative.pulsar.handlers.kop.KafkaProtocolHandler.TLS_HANDLER;

/**
 * A channel initializer that initialize channels for kafka protocol.
 */
public class KafkaProxyChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final int MAX_FRAME_LENGTH = 100 * 1024 * 1024; // 100MB

    @Getter
    private final PulsarAdmin pulsarAdmin;
    @Getter
    private final AuthenticationService authenticationService;
    @Getter
    private final KafkaServiceConfiguration kafkaConfig;

    @Getter
    private final boolean enableTls;
    @Getter
    private final EndPoint advertisedEndPoint;
    @Getter
    private final SslContextFactory.Server sslContextFactory;


    public KafkaProxyChannelInitializer(
                                   PulsarAdmin pulsarAdmin,
                                   AuthenticationService authenticationService,
                                   KafkaServiceConfiguration kafkaConfig,
                                   boolean enableTLS,
                                   EndPoint advertisedEndPoint) {
        super();
        this.authenticationService = authenticationService;
        this.pulsarAdmin = pulsarAdmin;
        this.kafkaConfig = kafkaConfig;
        this.enableTls = enableTLS;
        this.advertisedEndPoint = advertisedEndPoint;
        if (enableTls) {
            sslContextFactory = SSLUtils.createSslContextFactory(kafkaConfig);
        } else {
            sslContextFactory = null;
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        if (this.enableTls) {
            ch.pipeline().addLast(TLS_HANDLER, new SslHandler(SSLUtils.createSslEngine(sslContextFactory)));
        }
        String id = ch.remoteAddress() + "";
        ch.pipeline().addLast(new LengthFieldPrepender(4));
        ch.pipeline().addLast("frameDecoder",
            new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
        ch.pipeline().addLast("handler",
            new KafkaProxyRequestHandler(id, pulsarAdmin, authenticationService, kafkaConfig,
                    enableTls, advertisedEndPoint));
    }

}
