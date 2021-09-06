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
package io.streamnative.pulsar.handlers.kop.proxy;

import io.streamnative.pulsar.handlers.kop.DifferentNamespaceTestBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * test topics in different namespaces with `entryFormat=pulsar`.
 */
public class DifferentNamespacePulsarProxyTest extends DifferentNamespaceTestBase {

    public DifferentNamespacePulsarProxyTest() {
        super("pulsar");
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.setup();
        startProxy();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        stopProxy();
        super.cleanup();
    }

    protected int getClientPort() {
        return getKafkaProxyPort();
    }
}
