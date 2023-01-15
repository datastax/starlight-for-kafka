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

import com.google.common.collect.Sets;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Same as {@link SaslOAuthKopHandlersTest} but using the proxy.
 *
 * Proxy -> Broker AUTH is with OAUTH2
 */
@Slf4j
public class SaslOAuthKopHandlersProxyTest extends SaslOAuthKopHandlersTest {
    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.setup();
        startProxy();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        stopProxy();
        super.cleanup();
    }

    @Override
    protected void overrideBrokerConfig(KafkaServiceConfiguration conf) {
        // setup PROXY connection for broker
        conf.setSuperUserRoles(Sets.newHashSet(ADMIN_USER, PROXY_USER));
        conf.setProxyRoles(Sets.newHashSet(PROXY_USER));
        // proxy -> broker authentication is with OAUTHBEARER
    }

    @Override
    protected void prepareProxyConfiguration(Properties config) throws Exception {
        config.put("authenticationEnabled", conf.isAuthenticationEnabled() + "");
        config.put("authorizationEnabled", conf.isAuthorizationEnabled() + "");
        // proxy supports only OAUTHBEARER
        config.put("saslAllowedMechanisms", "OAUTHBEARER");
        config.put("kopOauth2ConfigFile", conf.getKopOauth2ConfigFile());
        config.put("kopOauth2Properties", conf.getKopOauth2Properties());
        if (conf.getKopOauth2AuthenticateCallbackHandler() != null) {
            config.put("kopOauth2AuthenticateCallbackHandler", conf.getKopOauth2AuthenticateCallbackHandler());
        }
        config.put("kafkaProxySuperUserRole", ADMIN_USER);

        config.put("authenticationProviders", conf.getAuthenticationProviders().stream().collect(Collectors.joining()));

        // PROXY -> BROKER uses OAUTHBEARER
        config.put("brokerClientAuthenticationPlugin", AuthenticationOAuth2.class.getName());
        config.put("brokerClientAuthenticationParameters", String.format("{\"type\":\"client_credentials\","
                        + "\"privateKey\":\"%s\",\"issuerUrl\":\"%s\",\"audience\":\"%s\"}",
                proxyCredentialPath, ISSUER_URL, AUDIENCE));
    }

    protected int getClientPort() {
        return getKafkaProxyPort();
    }

    @Test(enabled = false)
    public void testGrantAndRevokePermission() throws Exception {
    }

    @Test(enabled = false)
    public void testAuthenticationHasException() throws Exception {
    }
}
