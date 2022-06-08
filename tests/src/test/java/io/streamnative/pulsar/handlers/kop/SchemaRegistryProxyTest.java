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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;

/**
 * Test for KoP with Confluent Schema Registry.
 */
@Slf4j
public class SchemaRegistryProxyTest extends SchemaRegistryTest {

    @Factory
    public static Object[] instances() {
        return new Object[] {
                new SchemaRegistryProxyTest("pulsar", false),
                new SchemaRegistryProxyTest("pulsar", true),
                new SchemaRegistryProxyTest("kafka", false),
                new SchemaRegistryProxyTest("kafka", true)
        };
    }

    public SchemaRegistryProxyTest(String entryFormat, boolean applyAvroSchemaOnDecode) {
        super(entryFormat, applyAvroSchemaOnDecode);
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.setup();
        startProxy();
        // use proxy
        bootstrapServers = "localhost:" + getClientPort();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        log.info("STOPPING");
        stopProxy();
        super.cleanup();
    }

    protected int getClientPort() {
        return getKafkaProxyPort();
    }
}
