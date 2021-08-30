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
import lombok.SneakyThrows;
import org.apache.pulsar.proxy.protocol.ProtocolHandler;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.apache.pulsar.proxy.server.ProxyService;

import java.net.InetSocketAddress;
import java.util.Map;

public class KafkaProtocolProxyHandler implements ProtocolHandler  {

    private KafkaProtocolProxyMain protocolHandlerCore;

    public KafkaProtocolProxyHandler(){
        protocolHandlerCore = new KafkaProtocolProxyMain();
    }


    /**
     * Returns the unique protocol name. For example, `kafka-v2` for protocol handler for Kafka v2 protocol.
     */
    public String protocolName() {
        return "kafka";
    }

    /**
     * Verify if the protocol can speak the given <tt>protocol</tt>.
     *
     * @param protocol the protocol to verify
     * @return true if the protocol handler can handle the given protocol, otherwise false.
     */
    public boolean accept(String protocol) {
        return protocolName().equals(protocol);
    }

    /**
     * Initialize the protocol handler when the protocol is constructed from reflection.
     *
     * <p>The initialize should initialize all the resources required for serving the protocol
     * handler but don't start those resources until {@link #start(ProxyService)} is called.
     *
     * @param conf broker service configuration
     * @throws Exception when fail to initialize the protocol handler.
     */
    public void initialize(ProxyConfiguration conf) throws Exception {
        protocolHandlerCore.initialize(conf);
    }

    /**
     * Start the protocol handler with the provided broker service.
     *
     * <p>The broker service provides the accesses to the Pulsar components such as load
     * manager, namespace service, managed ledger and etc.
     *
     * @param service the broker service to start with.
     */
    public void start(ProxyService service) {
        protocolHandlerCore.start();
    }

    /**
     * Create the list of channel initializers for the ports that this protocol handler
     * will listen on.
     *
     * <p>NOTE: this method is called after {@link #start(ProxyService)}.
     *
     * @return the list of channel initializers for the ports that this protocol handler listens on.
     */
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        return protocolHandlerCore.newChannelInitializers();
    }

    @Override
    @SneakyThrows
    public void close() {
        protocolHandlerCore.close();
    }
}
