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

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class DistributedClusterProxyTest extends DistributedClusterTest {
    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.setup();

        startProxy();
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
}
