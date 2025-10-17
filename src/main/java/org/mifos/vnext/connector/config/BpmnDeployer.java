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
import io.camunda.zeebe.client.api.response.DeploymentEvent;
import io.camunda.zeebe.client.api.response.Process;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;
import java.io.IOException;

@Component
public class BpmnDeployer {

    private static final Logger logger = LoggerFactory.getLogger(BpmnDeployer.class);

    @Autowired
    private ZeebeClient zeebeClient;

    @EventListener(ApplicationReadyEvent.class)
    public void deployProcesses() {
        logger.info("Starting BPMN processes deployment...");

        try {
            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Resource[] resources = resolver.getResources("classpath*:processes/*.bpmn");

            if (resources.length == 0) {
                logger.warn("No BPMN files found in processes/ directory");
                return;
            }

            for (Resource resource : resources) {
                if (resource.exists() && resource.isReadable()) {
                    try {
                        String filename = resource.getFilename();
                        logger.info("Deploying process: {}", filename);

                        DeploymentEvent deployment = zeebeClient.newDeployResourceCommand()
                                .addResourceFromClasspath("processes/" + filename)
                                .send()
                                .join();

                        deployment.getProcesses().forEach(process ->
                                logger.info("Successfully deployed: {} (v{})",
                                        process.getBpmnProcessId(),
                                        process.getVersion())
                        );

                    } catch (Exception e) {
                        logger.error("Failed to deploy: {}", resource.getFilename(), e);
                    }
                }
            }

            logger.info("BPMN deployment completed. Total processes: {}", resources.length);

            // Listar procesos desplegados
            listDeployedProcesses();

        } catch (IOException e) {
            logger.error("Error accessing BPMN files", e);
        }
    }

    private void listDeployedProcesses() {
        try {

            var topology = zeebeClient.newTopologyRequest().send().join();
            logger.info("Connected to Zeebe cluster with {} brokers", topology.getBrokers().size());
        } catch (Exception e) {
            logger.error("Error checking topology", e);
        }
    }
}