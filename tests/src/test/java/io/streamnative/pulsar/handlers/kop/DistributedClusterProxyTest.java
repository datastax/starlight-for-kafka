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

import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DistributedClusterProxyTest extends DistributedClusterTest {
    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.setup();
        startProxy();
    }

    protected void beforeStartingProxy(ProxyConfiguration proxyConfiguration) throws Exception {
        // we have to give the addresses of all the brokers, because in some tests we are stopping one of them
        proxyConfiguration.setBrokerWebServiceURL("http://"
                + "localhost:" + primaryBrokerWebservicePort + ";"
                + "localhost:" + secondaryBrokerWebservicePort);
    }

    @AfterMethod(timeOut = 30000, alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        stopProxy();
        super.cleanup();
    }

    protected int getClientPort() {
        return getKafkaProxyPort();
    }

    protected String computeKafkaProxyBrokerPortToKopMapping() {
        return primaryBrokerPort + "=" + primaryKafkaBrokerPort + "," +
                secondaryBrokerPort + "=" + secondaryKafkaBrokerPort;
    }

    @Test(timeOut = 180000, priority = -1)
    @Override
    public void testMultiBrokerUnloadReload() throws Exception {
        super.testMultiBrokerUnloadReload();
    }

    @Test(timeOut = 180000)
    @Override
    public void testOneBrokerShutdown() throws Exception {
        super.testOneBrokerShutdown();
    }
}
