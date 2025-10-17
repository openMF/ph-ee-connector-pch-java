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
package org.mifos.vnext.connector.worker;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.worker.JobWorker;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobHandler;
import io.camunda.zeebe.client.api.worker.JobClient;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.mifos.vnext.connector.client.PchRestClient;
import org.mifos.vnext.connector.dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Component
public class ZeebeWorker {

    private static final Logger logger = LoggerFactory.getLogger(ZeebeWorker.class);

    @Value("${zeebe.client.worker.default-name:vnext-rest-workers}")
    private String workerName;

    @Autowired
    private ZeebeClient zeebeClient;

    @Autowired
    private PchRestClient vnextRestClient;

    private JobWorker participantLookupWorker;
    private JobWorker partyLookupWorker;
    private JobWorker executeTransferWorker;

    @PostConstruct
    public void init() {
        logger.info("Initializing Vnext REST Zeebe Workers...");
        setupZeebeWorkers();
        logger.info("Vnext REST Zeebe Workers initialized successfully");
    }

    @PreDestroy
    public void cleanup() {
        logger.info("Shutting down Vnext REST Zeebe Workers...");
        closeWorker(participantLookupWorker, "participant lookup");
        closeWorker(partyLookupWorker, "party lookup");
        closeWorker(executeTransferWorker, "execute transfer");
        logger.info("Vnext REST Zeebe Workers shutdown completed");
    }

    private void closeWorker(JobWorker worker, String workerType) {
        if (worker != null) {
            try {
                worker.close();
                logger.info("{} worker closed successfully", workerType);
            } catch (Exception e) {
                logger.error("Error closing {} worker", workerType, e);
            }
        }
    }

    private void setupZeebeWorkers() {
        // Worker para participant lookup
        participantLookupWorker = zeebeClient
                .newWorker()
                .jobType("vnext-participant-lookup")
                .handler(new ParticipantLookupHandler())
                .name(workerName + "-participant")
                .timeout(Duration.ofSeconds(30))
                .maxJobsActive(10)
                .open();

        // Worker para party lookup
        partyLookupWorker = zeebeClient
                .newWorker()
                .jobType("vnext-party-lookup")
                .handler(new PartyLookupHandler())
                .name(workerName + "-party")
                .timeout(Duration.ofSeconds(30))
                .maxJobsActive(10)
                .open();

        // Worker para execute transfer
        executeTransferWorker = zeebeClient
                .newWorker()
                .jobType("vnext-execute-transfer")
                .handler(new ExecuteTransferHandler())
                .name(workerName + "-transfer")
                .timeout(Duration.ofSeconds(60))
                .maxJobsActive(5)
                .open();

        logger.info("Registered REST workers for: participant-lookup, party-lookup, execute-transfer");
    }

    private class ParticipantLookupHandler implements JobHandler {
        @Override
        public void handle(JobClient client, ActivatedJob job) {
            final long jobKey = job.getKey();
            try {
                logger.info("[Job-{}] Processing participant lookup via REST", jobKey);

                Map<String, Object> variables = job.getVariablesAsMap();
                ParticipantRequest participantRequest = mapToParticipantRequest(variables);

                ParticipantResponse participantResponse = vnextRestClient.getParticipant(participantRequest);

                Map<String, Object> resultVariables = new HashMap<>();
                resultVariables.put("participantResponse", participantResponse);
                resultVariables.put("executionStatus", participantResponse.isExecutionStatus());

                if (participantResponse.isExecutionStatus()) {
                    client.newCompleteCommand(jobKey)
                            .variables(resultVariables)
                            .send()
                            .join();
                    logger.info("[Job-{}] Participant lookup completed successfully via REST", jobKey);
                } else {
                    client.newThrowErrorCommand(jobKey)
                            .errorCode("PARTICIPANT_LOOKUP_ERROR")
                            .errorMessage(participantResponse.getSystemMessage())
                            .variables(resultVariables)
                            .send()
                            .join();
                    logger.error("[Job-{}] Participant lookup failed via REST: {}", jobKey, participantResponse.getSystemMessage());
                }

            } catch (Exception e) {
                logger.error("[Job-{}] Error processing participant lookup via REST", jobKey, e);
                handleJobFailure(client, job, e);
            }
        }
    }

    private class PartyLookupHandler implements JobHandler {
        @Override
        public void handle(JobClient client, ActivatedJob job) {
            final long jobKey = job.getKey();
            try {
                logger.info("[Job-{}] Processing party lookup via REST", jobKey);

                Map<String, Object> variables = job.getVariablesAsMap();
                PartyRequest partyRequest = mapToPartyRequest(variables);

                PartyResponseDto partyResponse = vnextRestClient.getPartyInfo(partyRequest);

                Map<String, Object> resultVariables = new HashMap<>();
                resultVariables.put("partyResponse", partyResponse);
                resultVariables.put("executionStatus", partyResponse.isExecutionStatus());

                if (partyResponse.isExecutionStatus()) {
                    client.newCompleteCommand(jobKey)
                            .variables(resultVariables)
                            .send()
                            .join();
                    logger.info("[Job-{}] Party lookup completed successfully via REST", jobKey);
                } else {
                    client.newThrowErrorCommand(jobKey)
                            .errorCode("PARTY_LOOKUP_ERROR")
                            .errorMessage(partyResponse.getErrorMessage())
                            .variables(resultVariables)
                            .send()
                            .join();
                    logger.error("[Job-{}] Party lookup failed via REST: {}", jobKey, partyResponse.getErrorMessage());
                }

            } catch (Exception e) {
                logger.error("[Job-{}] Error processing party lookup via REST", jobKey, e);
                handleJobFailure(client, job, e);
            }
        }
    }

    private class ExecuteTransferHandler implements JobHandler {
        @Override
        public void handle(JobClient client, ActivatedJob job) {
            final long jobKey = job.getKey();
            try {
                logger.info("[Job-{}] Processing execute transfer via REST", jobKey);

                Map<String, Object> variables = job.getVariablesAsMap();
                TransferRequest transferRequest = mapToTransferRequest(variables);

                TransferResponseDto transferResponse = vnextRestClient.executeTransfer(transferRequest);

                Map<String, Object> resultVariables = new HashMap<>();
                resultVariables.put("transferResponse", transferResponse);
                resultVariables.put("executionStatus", transferResponse.isExecutionStatus());
                resultVariables.put("transferId", transferResponse.getTransactionId());

                if (transferResponse.isExecutionStatus()) {
                    client.newCompleteCommand(jobKey)
                            .variables(resultVariables)
                            .send()
                            .join();
                    logger.info("[Job-{}] Transfer executed successfully via REST, transferId: {}",
                            jobKey, transferResponse.getTransactionId());
                } else {
                    client.newThrowErrorCommand(jobKey)
                            .errorCode("TRANSFER_EXECUTION_ERROR")
                            .errorMessage(transferResponse.getSystemMessage())
                            .variables(resultVariables)
                            .send()
                            .join();
                    logger.error("[Job-{}] Transfer execution failed via REST: {}", jobKey, transferResponse.getSystemMessage());
                }

            } catch (Exception e) {
                logger.error("[Job-{}] Error processing transfer execution via REST", jobKey, e);
                handleJobFailure(client, job, e);
            }
        }
    }

    private void handleJobFailure(JobClient client, ActivatedJob job, Exception e) {
        int remainingRetries = job.getRetries() - 1;
        client.newFailCommand(job.getKey())
                .retries(remainingRetries)
                .errorMessage(e.getMessage())
                .send()
                .join();
        logger.warn("[Job-{}] Job failed, {} retries remaining", job.getKey(), remainingRetries);
    }

    //mapeo desde variables de Zeebe a DTOs
    private ParticipantRequest mapToParticipantRequest(Map<String, Object> variables) {
        ParticipantRequest request = new ParticipantRequest();
        if (variables.containsKey("participantId")) {
            request.setParticipantId((String) variables.get("participantId"));
        }
        if (variables.containsKey("participantType")) {
            request.setParticipantType((String) variables.get("participantType"));
        }
        if (variables.containsKey("currencyCode")) {
            request.setCurrencyCode((String) variables.get("currencyCode"));
        }

        return request;
    }

    //mapeo desde variables de Zeebe a DTOs
    private PartyRequest mapToPartyRequest(Map<String, Object> variables) {
        PartyRequest request = new PartyRequest();
        if (variables.containsKey("partyId")) {
            request.setPartyId((String) variables.get("partyId"));
        }
        if (variables.containsKey("partyIdType")) {
            request.setPartyIdType((String) variables.get("partyIdType"));
        }
        if (variables.containsKey("ownerFspId")) {
            request.setOwnerFspId((String) variables.get("ownerFspId"));
        }
        if (variables.containsKey("currencyCode")) {
            request.setCurrencyCode((String) variables.get("currencyCode"));
        }
        return request;
    }

    //mapeo desde variables de Zeebe a DTOs
    private TransferRequest mapToTransferRequest(Map<String, Object> variables) {
        TransferRequest request = new TransferRequest();

        logger.info("Mapping TransferRequestDto from variables: {}", variables);


        if (variables.containsKey("homeTransactionId")) {
            request.setHomeTransactionId((String) variables.get("homeTransactionId"));
        }

        if (variables.containsKey("amountType")) {
            request.setAmountType((String) variables.get("amountType"));
        }

        if (variables.containsKey("note")) {
            request.setNote((String) variables.get("note"));
        }

        // Mapear objeto 'from'
        if (variables.containsKey("from")) {
            Object fromObj = variables.get("from");
            if (fromObj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> fromMap = (Map<String, Object>) fromObj;
                request.setFrom(mapToPartyDto(fromMap));
            }
        }

        // Mapear objeto 'to'
        if (variables.containsKey("to")) {
            Object toObj = variables.get("to");
            if (toObj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> toMap = (Map<String, Object>) toObj;
                request.setTo(mapToPartyDto(toMap));
            }
        }

        // Mapear objeto 'amount'
        if (variables.containsKey("amount")) {
            Object amountObj = variables.get("amount");
            if (amountObj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> amountMap = (Map<String, Object>) amountObj;
                request.setAmount(mapToAmountDto(amountMap));
            }
        }

        // Mapear objeto 'transactionType'
        if (variables.containsKey("transactionType")) {
            Object transactionTypeObj = variables.get("transactionType");
            if (transactionTypeObj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> transactionTypeMap = (Map<String, Object>) transactionTypeObj;
                request.setTransactionType(mapToTransactionTypeDto(transactionTypeMap));
            }
        }

        logger.info("Mapped TransferRequestDto: homeTransactionId={}, from={}, to={}",
                request.getHomeTransactionId(),
                request.getFrom() != null ? request.getFrom().getFspId() : "null",
                request.getTo() != null ? request.getTo().getFspId() : "null");

        return request;
    }

    private PartyDto mapToPartyDto(Map<String, Object> partyMap) {
        PartyDto party = new PartyDto();

        if (partyMap.containsKey("fspId")) {
            party.setFspId((String) partyMap.get("fspId"));
        }

        if (partyMap.containsKey("idType")) {
            party.setIdType((String) partyMap.get("idType"));
        }

        if (partyMap.containsKey("idValue")) {
            party.setIdValue((String) partyMap.get("idValue"));
        }

        logger.debug("Mapped PartyDto: fspId={}, idType={}, idValue={}",
                party.getFspId(), party.getIdType(), party.getIdValue());

        return party;
    }

    private AmountDto mapToAmountDto(Map<String, Object> amountMap) {
        AmountDto amount = new AmountDto();

        if (amountMap.containsKey("currencyCode")) {
            amount.setCurrencyCode((String) amountMap.get("currencyCode"));
        }

        if (amountMap.containsKey("amount")) {
            Object amountValue = amountMap.get("amount");
            if (amountValue instanceof String) {
                amount.setAmount((String) amountValue);
            } else if (amountValue instanceof Number) {
                amount.setAmount(String.valueOf(amountValue));
            }
        }

        logger.debug("Mapped AmountDto: currencyCode={}, amount={}",
                amount.getCurrencyCode(), amount.getAmount());

        return amount;
    }

    private TransactionTypeDto mapToTransactionTypeDto(Map<String, Object> transactionTypeMap) {
        TransactionTypeDto transactionType = new TransactionTypeDto();

        if (transactionTypeMap.containsKey("scenario")) {
            transactionType.setScenario((String) transactionTypeMap.get("scenario"));
        }

        if (transactionTypeMap.containsKey("subScenario")) {
            transactionType.setSubScenario((String) transactionTypeMap.get("subScenario"));
        }

        if (transactionTypeMap.containsKey("initiator")) {
            transactionType.setInitiator((String) transactionTypeMap.get("initiator"));
        }

        if (transactionTypeMap.containsKey("initiatorType")) {
            transactionType.setInitiatorType((String) transactionTypeMap.get("initiatorType"));
        }

        logger.debug("Mapped TransactionTypeDto: scenario={}, initiator={}",
                transactionType.getScenario(), transactionType.getInitiator());

        return transactionType;
    }
}