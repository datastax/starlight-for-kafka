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
package io.streamnative.pulsar.handlers.kop.compatibility.saslplain;

import java.util.Properties;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class SaslPlainEndToEndPulsarProxyTest extends SaslPlainEndToEndTest {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.setup();

        // when using PulsarAdmin you need to be Tenant Admin in order to
        // list topic partitions
        TenantInfo tenantInfo = admin.tenants().getTenantInfo(TENANT);
        tenantInfo.getAdminRoles().add(SIMPLE_USER);
        admin.tenants().updateTenant(TENANT, tenantInfo);

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
