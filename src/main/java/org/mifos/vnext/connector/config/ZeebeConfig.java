/**
 * Licensed to the Mifos Initiative under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.mifos.vnext.connector.config;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.ZeebeClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class ZeebeConfig {

    @Value("${zeebe.client.broker.gateway-address:localhost:26500}")
    private String gatewayAddress;

    @Value("${zeebe.client.security.plaintext:true}")
    private boolean usePlaintext;

    @Bean
    public ZeebeClient zeebeClient() {
        ZeebeClientBuilder clientBuilder = ZeebeClient.newClientBuilder()
                .gatewayAddress(gatewayAddress)
                .defaultJobPollInterval(Duration.ofMillis(100))
                .defaultJobWorkerMaxJobsActive(10)
                .defaultJobTimeout(Duration.ofSeconds(60))
                .defaultJobWorkerName("vnext-rest-workers");

        if (usePlaintext) {
            clientBuilder.usePlaintext();
        }

        return clientBuilder.build();
    }
}