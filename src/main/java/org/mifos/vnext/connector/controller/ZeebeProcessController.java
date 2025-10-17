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

import org.mifos.vnext.connector.service.ZeebeProcessService;
import org.mifos.vnext.connector.dto.ParticipantRequest;
import org.mifos.vnext.connector.dto.PartyRequest;
import org.mifos.vnext.connector.dto.TransferRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("/zeebe")
public class ZeebeProcessController {

    private static final Logger logger = LoggerFactory.getLogger(ZeebeProcessController.class);

    @Autowired
    private ZeebeProcessService zeebeProcessService;

    @Autowired
    private ObjectMapper objectMapper;

    @PostMapping("/start/participant-lookup")
    public ResponseEntity<Map<String, Object>> startParticipantLookup(
            @RequestBody ParticipantRequest request) {
        logger.info("Participant Request: {}",request.toString());

        try {
            String businessKey = request.getBusinessKey() != null ?
                    request.getBusinessKey() : "participant-" + UUID.randomUUID().toString();

            Map<String, Object> variables = objectMapper.convertValue(request, Map.class);

            var result = zeebeProcessService.startParticipantLookupProcess(businessKey, variables)
                    .get(); // Blocking for simplicity, you might want to make this async

            Map<String, Object> response = new HashMap<>();
            response.put("processInstanceKey", result.getProcessInstanceKey());
            response.put("businessKey", businessKey);
            response.put("status", "STARTED");

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Failed to start participant lookup process", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/start/party-info-lookup")
    public ResponseEntity<Map<String, Object>> startPartyInfoLookup(
            @RequestBody PartyRequest request) {
        logger.info("Party lookup Request: {}",request.toString());

        try {
            String businessKey = request.getBusinessKey() != null ?
                    request.getBusinessKey() : "party-" + UUID.randomUUID().toString();

            Map<String, Object> variables = objectMapper.convertValue(request, Map.class);

            var result = zeebeProcessService.startPartyInfoLookupProcess(businessKey, variables)
                    .get();

            Map<String, Object> response = new HashMap<>();
            response.put("processInstanceKey", result.getProcessInstanceKey());
            response.put("businessKey", businessKey);
            response.put("status", "STARTED");

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Failed to start party info lookup process", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/start/transfer-execution")
    public ResponseEntity<Map<String, Object>> startTransferExecution(
            @RequestBody TransferRequest request) {

        try {
            String businessKey = request.getBusinessKey() != null ?
                    request.getBusinessKey() : "transfer-" + UUID.randomUUID().toString();

            Map<String, Object> variables = objectMapper.convertValue(request, Map.class);

            var result = zeebeProcessService.startTransferExecutionProcess(businessKey, variables)
                    .get();

            Map<String, Object> response = new HashMap<>();
            response.put("processInstanceKey", result.getProcessInstanceKey());
            response.put("businessKey", businessKey);
            response.put("status", "STARTED");

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Failed to start transfer execution process", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }
}