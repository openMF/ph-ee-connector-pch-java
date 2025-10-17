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
package org.mifos.vnext.connector.controller;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.DeploymentEvent;
import io.camunda.zeebe.client.api.response.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/deployment")
public class DeploymentController {

    private static final Logger logger = LoggerFactory.getLogger(DeploymentController.class);

    @Autowired
    private ZeebeClient zeebeClient;

    @GetMapping("/status")
    public Map<String, Object> getDeploymentStatus() {
        Map<String, Object> status = new HashMap<>();

        try {
            // Verificar conexión con Zeebe
            Topology topology = zeebeClient.newTopologyRequest().send().join();
            status.put("zeebeConnected", true);
            status.put("brokers", topology.getBrokers().size());
            status.put("clusterSize", topology.getClusterSize());
            status.put("partitionsCount", topology.getPartitionsCount());
            status.put("replicationFactor", topology.getReplicationFactor());

            status.put("message", "Zeebe cluster is healthy");

        } catch (Exception e) {
            status.put("zeebeConnected", false);
            status.put("error", e.getMessage());
            status.put("message", "Cannot connect to Zeebe cluster");
        }

        return status;
    }

    @PostMapping("/redeploy")
    public Map<String, Object> redeployProcesses() {
        Map<String, Object> result = new HashMap<>();

        try {
            DeploymentEvent deployment = zeebeClient.newDeployResourceCommand()
                    .addResourceFromClasspath("processes/participant-lookup-process.bpmn")
                    .addResourceFromClasspath("processes/party-info-lookup-process.bpmn")
                    .addResourceFromClasspath("processes/transfer-execution-process.bpmn")
                    .addResourceFromClasspath("processes/customer-accounts-process.bpmn")
                    .send()
                    .join();

            result.put("success", true);
            result.put("message", "Successfully redeployed " + deployment.getProcesses().size() + " processes");
            result.put("deployedProcesses", deployment.getProcesses().size());

            // Listar procesos desplegados
            deployment.getProcesses().forEach(process ->
                    logger.info("Redeployed: {} (v{})", process.getBpmnProcessId(), process.getVersion())
            );

        } catch (Exception e) {
            logger.error("Error redeploying processes", e);
            result.put("success", false);
            result.put("error", e.getMessage());
        }

        return result;
    }

    @PostMapping("/deploy-single")
    public Map<String, Object> deploySingleProcess(@RequestParam String processName) {
        Map<String, Object> result = new HashMap<>();

        try {
            DeploymentEvent deployment = zeebeClient.newDeployResourceCommand()
                    .addResourceFromClasspath("processes/" + processName + ".bpmn")
                    .send()
                    .join();

            result.put("success", true);
            result.put("message", "Successfully deployed " + processName);
            result.put("processes", deployment.getProcesses().size());

            deployment.getProcesses().forEach(process ->
                    result.put("deployedProcess",
                            Map.of(
                                    "bpmnProcessId", process.getBpmnProcessId(),
                                    "version", process.getVersion(),
                                    "processDefinitionKey", process.getProcessDefinitionKey()
                            )
                    )
            );

        } catch (Exception e) {
            logger.error("Error deploying process: {}", processName, e);
            result.put("success", false);
            result.put("error", e.getMessage());
        }

        return result;
    }
}