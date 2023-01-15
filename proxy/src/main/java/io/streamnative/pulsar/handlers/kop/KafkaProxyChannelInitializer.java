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
import io.streamnative.pulsar.handlers.kop.utils.ssl.SSLUtils;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import lombok.Getter;
import org.apache.kafka.common.Node;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.client.api.Authentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A channel initializer that initialize channels for kafka protocol.
 */
public class KafkaProxyChannelInitializer extends ChannelInitializer<SocketChannel> {

    private static final Logger log = LoggerFactory.getLogger(KafkaProxyChannelInitializer.class);

    public static final int MAX_FRAME_LENGTH = 100 * 1024 * 1024; // 100MB

    @Getter
    private final KafkaProtocolProxyMain.PulsarAdminProvider pulsarAdmin;
    @Getter
    private final AuthenticationService authenticationService;
    private final AuthorizationService authorizationService;
    @Getter
    private final KafkaServiceConfiguration kafkaConfig;
    @Getter
    private final boolean enableTls;
    @Getter
    private final EndPoint advertisedEndPoint;
    private final SSLUtils.ServerSideTLSSupport tlsSupport;
    private final RequestStats requestStats;
    private final Function<String, String> brokerAddressMapper;
    private final ConcurrentHashMap<String, Node> topicsLeaders;
    private final Authentication authentication;

    public KafkaProxyChannelInitializer(
            KafkaProtocolProxyMain.PulsarAdminProvider pulsarAdmin,
            AuthenticationService authenticationService,
            AuthorizationService authorizationService,
            Authentication authentication,
            KafkaServiceConfiguration serviceConfig,
            boolean enableTLS,
            EndPoint advertisedEndPoint,
            Function<String, String> brokerAddressMapper,
            ConcurrentHashMap<String, Node> topicsLeaders,
            RequestStats requestStats) {
        super();
        this.topicsLeaders = topicsLeaders;
        this.requestStats = requestStats;
        this.authentication = authentication;
        this.brokerAddressMapper = brokerAddressMapper;
        this.authenticationService = authenticationService;
        this.authorizationService = authorizationService;
        this.pulsarAdmin = pulsarAdmin;
        this.kafkaConfig = serviceConfig;
        this.enableTls = enableTLS;
        this.advertisedEndPoint = advertisedEndPoint;

        // we are using Pulsar Proxy TLS configuration, not KOP
        // this way we can use the same configuration of conf/proxy.conf
        if (enableTls) {
            tlsSupport = new SSLUtils.ServerSideTLSSupport(kafkaConfig);
        } else {
            tlsSupport = null;
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        if (tlsSupport != null) {
            tlsSupport.addTlsHandler(ch);
        }
        String id = ch.remoteAddress() + "";
        ch.pipeline().addLast(new LengthFieldPrepender(4));
        ch.pipeline().addLast("frameDecoder",
                new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
        ch.pipeline().addLast("handler",
                new KafkaProxyRequestHandler(id, pulsarAdmin, authenticationService, authorizationService, kafkaConfig,
                        authentication,
                        // use the same eventloop to preserve ordering
                        enableTls, advertisedEndPoint, brokerAddressMapper, ch.eventLoop(),
                        requestStats, topicsLeaders));
    }

}
