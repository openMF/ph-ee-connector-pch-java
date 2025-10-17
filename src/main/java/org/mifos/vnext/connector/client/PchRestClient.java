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
package org.mifos.vnext.connector.client;

import org.mifos.vnext.connector.dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

@Component
public class PchRestClient {

    private static final Logger logger = LoggerFactory.getLogger(PchRestClient.class);

    private final WebClient webClient;

    public PchRestClient(@Value("${app.vnext.base-url:http://localhost:8011}") String baseUrl) {
        this.webClient = WebClient.builder()
                .baseUrl(baseUrl)
                .defaultHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    public ParticipantResponse getParticipant(ParticipantRequest request) {
        logger.info("Calling participant endpoint with request: {}", request.toString());
        ParticipantDto participantDto= new ParticipantDto();
        participantDto.setPartyId(request.getParticipantId());
        participantDto.setPartyIdType(request.getParticipantType());
        participantDto.setCurrencyCode(request.getCurrencyCode());
        logger.info("ParticipantDto Request: {}",participantDto.toString());

        return webClient.post()
                .uri("/participant")
                .bodyValue(participantDto)
                .retrieve()
                .bodyToMono(ParticipantResponse.class)
                .doOnSuccess(response -> logger.info("Participant response: {}", response))
                .doOnError(error -> logger.error("Error calling participant endpoint", error))
                .block();
    }

    public PartyResponseDto getPartyInfo(PartyRequest request) {
        logger.info("Calling partyinfo endpoint with request: {}", request);
        PartyRequestDto partyRequestDto =new PartyRequestDto();
        partyRequestDto.setPartyId(request.getPartyId());
        partyRequestDto.setPartyIdType(request.getPartyIdType());
        partyRequestDto.setCurrencyCode(request.getCurrencyCode());
        partyRequestDto.setOwnerFspId(request.getOwnerFspId());
        logger.info("Calling partyinfo endpoint with PartyRequestDto: {}", partyRequestDto);


        return webClient.post()
                .uri("/partyinfo")
                .bodyValue(partyRequestDto)
                .retrieve()
                .bodyToMono(PartyResponseDto.class)
                .doOnSuccess(response -> logger.info("Party info response: {}", response))
                .doOnError(error -> logger.error("Error calling partyinfo endpoint", error))
                .block();
    }

    public TransferResponseDto executeTransfer(TransferRequest request) {
        logger.info("Calling executetransfer endpoint with request: {}", request);
        TransferRequestDto transferRequestDto = new TransferRequestDto();
        transferRequestDto.setHomeTransactionId(request.getHomeTransactionId());
        transferRequestDto.setFrom(request.getFrom());
        transferRequestDto.setTo(request.getTo());
        transferRequestDto.setAmountType(request.getAmountType());
        transferRequestDto.setAmount(request.getAmount());
        transferRequestDto.setTransactionType(request.getTransactionType());
        transferRequestDto.setNote(request.getNote());

        return webClient.post()
                .uri("/executetransfer")
                .bodyValue(transferRequestDto)
                .retrieve()
                .bodyToMono(TransferResponseDto.class)
                .doOnSuccess(response -> logger.info("Transfer response: {}", response))
                .doOnError(error -> logger.error("Error calling executetransfer endpoint", error))
                .block();
    }
}