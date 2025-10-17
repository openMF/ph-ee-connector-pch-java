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
package org.mifos.vnext.connector.service;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

@Service
public class ZeebeHealthCheck {

    private static final Logger logger = LoggerFactory.getLogger(ZeebeHealthCheck.class);

    @Autowired
    private ZeebeClient zeebeClient;

    @EventListener(ApplicationReadyEvent.class)
    public void checkZeebeConnection() {
        try {
            Topology topology = zeebeClient.newTopologyRequest().send().join();

            logger.info("Successfully connected to Zeebe cluster");
            logger.info("Cluster Info:");
            logger.info("   - Brokers: {}", topology.getBrokers().size());
            logger.info("   - Cluster Size: {}", topology.getClusterSize());
            logger.info("   - Partitions: {}", topology.getPartitionsCount());
            logger.info("   - Replication Factor: {}", topology.getReplicationFactor());

            topology.getBrokers().forEach(broker ->
                    logger.info("   - Broker {}:{} (v{})",
                            broker.getHost(),
                            broker.getPort(),
                            broker.getVersion())
            );

        } catch (Exception e) {
            logger.error("Cannot connect to Zeebe Broker at: {}",
                    zeebeClient.getConfiguration().getGatewayAddress());
            logger.error("   Error: {}", e.getMessage());
        }
    }
}