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

import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Unit test for {@link KafkaRequestHandler} but via Proxy.
 */
@Slf4j
public class KafkaRequestHandlerProxyTest extends KafkaRequestHandlerTest {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.setup();
        startProxy();
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
