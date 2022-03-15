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

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Testing multi tenant features on KoP.
 */
@Test
@Slf4j
public class SaslMultitenantProxyTest extends SaslMultitenantTest {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.setup();

        startProxy();
    }


    @Override
    protected void prepareProxyConfiguration(Properties conf) throws Exception {
        conf.put("authorizationEnabled", "true");
        conf.put("authenticationEnabled", "true");
        conf.put("kafkaProxySuperUserRole", ADMIN_USER);

        conf.put("authenticationProviders", AuthenticationProviderToken.class.getName());

        // the Proxy myst use a proxy token
        conf.put("brokerClientAuthenticationPlugin", AuthenticationToken.class.getName());
        conf.put("brokerClientAuthenticationParameters", "token:" + proxyToken);
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        stopProxy();
        super.cleanup();
    }

    protected int getClientPort() {
        return getKafkaProxyPort();
    }

}
