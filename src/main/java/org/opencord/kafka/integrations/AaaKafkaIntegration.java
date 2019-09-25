/*
 * Copyright 2018-present Open Networking Foundation
 *
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

package org.opencord.kafka.integrations;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.onosproject.net.AnnotationKeys;
import org.onosproject.net.device.DeviceService;
import org.opencord.aaa.AuthenticationEvent;
import org.opencord.aaa.AuthenticationEventListener;
import org.opencord.aaa.AuthenticationService;
import org.opencord.kafka.EventBusService;
import org.opencord.aaa.AuthenticationStatisticsEvent;
import org.opencord.aaa.AuthenticationStatisticsEventListener;
import org.opencord.aaa.AuthenticationStatisticsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opencord.aaa.AaaMachineStatisticsEvent;
import org.opencord.aaa.AaaMachineStatisticsEventListener;
import org.opencord.aaa.AaaMachineStatisticsService;
import org.opencord.aaa.AaaSupplicantMachineStats;

import java.time.Instant;

/**
 * Listens for AAA events and pushes them on a Kafka bus.
 */
@Component(immediate = true)
public class AaaKafkaIntegration {

    public Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected EventBusService eventBusService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected DeviceService deviceService;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policy = ReferencePolicy.DYNAMIC,
            bind = "bindAuthenticationService",
            unbind = "unbindAuthenticationService")
    protected AuthenticationService authenticationService;
    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policy = ReferencePolicy.DYNAMIC,
            bind = "bindAuthenticationStatService",
            unbind = "unbindAuthenticationStatService")
    protected AuthenticationStatisticsService authenticationStatisticsService;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policy = ReferencePolicy.DYNAMIC,
            bind = "bindAaaMachineStatisticsService",
            unbind = "unbindAaaMachineStatisticsService")
    protected AaaMachineStatisticsService machineStatisticsService;

    private final AuthenticationEventListener listener = new InternalAuthenticationListener();
    private final AuthenticationStatisticsEventListener authenticationStatisticsEventListener =
             new InternalAuthenticationStatisticsListner();
    private final AaaMachineStatisticsEventListener machineStatisticsEventListener =
            new InternalAaaMachineStatisticsListner();

    // topics
    private static final String TOPIC = "authentication.events";
    private static final String AUTHENTICATION_STATISTICS_TOPIC = "onos.aaa.stats.kpis";

    // auth event params
    private static final String TIMESTAMP = "timestamp";
    private static final String DEVICE_ID = "deviceId";
    private static final String PORT_NUMBER = "portNumber";
    private static final String SERIAL_NUMBER = "serialNumber";
    private static final String AUTHENTICATION_STATE = "authenticationState";

    // auth stats event params
    private static final String ACCEPT_RESPONSES_RX = "acceptResponsesRx";
    private static final String REJECT_RESPONSES_RX = "rejectResponsesRx";
    private static final String CHALLENGE_RESPONSES_RX = "challengeResponsesRx";
    private static final String ACCESS_REQUESTS_TX = "accessRequestsTx";
    private static final String INVALID_VALIDATORS_RX = "invalidValidatorsRx";
    private static final String UNKNOWN_TYPE_RX = "unknownTypeRx";
    private static final String PENDING_REQUESTS = "pendingRequests";
    private static final String DROPPED_RESPONSES_RX = "droppedResponsesRx";
    private static final String MALFORMED_RESPONSES_RX = "malformedResponsesRx";
    private static final String UNKNOWN_SERVER_RX = "unknownServerRx";
    private static final String REQUEST_RTT_MILLIS = "requestRttMillis";
    private static final String REQUEST_RE_TX = "requestReTx";

    //Supplicant machine stats event params
    private static final String SESSION_ID = "sessionId";
    private static final String SESSION_NAME = "sessionName";
    private static final String MAC_ADDRESS = "macAddress";
    private static final String EAPOL_TYPE = "eapolType";
    private static final String SESSION_DURATION = "sessionDuration";
    private static final String TOTAL_FRAMES_RX = "totalFramesRx";
    private static final String TOTAL_FRAMES_TX = "totalFramesTx";
    private static final String TOTAL_PACKETS_RX = "totalPacketsRx";
    private static final String TOTAL_PACKETS_TX = "totalFramesTx";
    private static final String SESSION_TERMINATE_REASON = "sessionTerminateReason";
    private static final String TOTAL_OCTETS_TX = "totalOctetsTx";
    private static final String TOTAL_OCTETS_RX = "totalOctetsRx";

    protected void bindAuthenticationService(AuthenticationService authenticationService) {
        log.info("bindAuthenticationService");
        if (this.authenticationService == null) {
            log.info("Binding AuthenticationService");
            this.authenticationService = authenticationService;
            log.info("Adding listener on AuthenticationService");
            authenticationService.addListener(listener);
        } else {
            log.warn("Trying to bind AuthenticationService but it is already bound");
        }
    }

    protected void unbindAuthenticationService(AuthenticationService authenticationService) {
        log.info("unbindAuthenticationService");
        if (this.authenticationService == authenticationService) {
            log.info("Unbinding AuthenticationService");
            this.authenticationService = null;
            log.info("Removing listener on AuthenticationService");
            authenticationService.removeListener(listener);
        } else {
            log.warn("Trying to unbind AuthenticationService but it is already unbound");
        }
    }

    protected void bindAuthenticationStatService(AuthenticationStatisticsService authenticationStatisticsService) {
        log.info("bindAuthenticationStatService");
        if (this.authenticationStatisticsService == null) {
            log.info("Binding AuthenticationStastService");
            this.authenticationStatisticsService = authenticationStatisticsService;
            log.info("Adding listener on AuthenticationStatService");
            authenticationStatisticsService.addListener(authenticationStatisticsEventListener);
        } else {
            log.warn("Trying to bind AuthenticationStatService but it is already bound");
        }
    }

    protected void unbindAuthenticationStatService(AuthenticationStatisticsService authenticationStatisticsService) {
        log.info("unbindAuthenticationStatService");
        if (this.authenticationStatisticsService == authenticationStatisticsService) {
            log.info("Unbinding AuthenticationStatService");
            this.authenticationStatisticsService = null;
            log.info("Removing listener on AuthenticationStatService");
            authenticationStatisticsService.removeListener(authenticationStatisticsEventListener);
        } else {
            log.warn("Trying to unbind AuthenticationStatService but it is already unbound");
        }
    }

    protected void bindAaaMachineStatisticsService(AaaMachineStatisticsService machineStatisticsService) {
        log.info("bindAaaMachineStatisticsService");
        if (this.machineStatisticsService == null) {
            log.info("Binding AaaMachineStatisticsService");
            this.machineStatisticsService = machineStatisticsService;
            log.info("Adding listener on AaaMachineStatisticsService");
            machineStatisticsService.addListener(machineStatisticsEventListener);
        } else {
            log.warn("Trying to bind AaaMachineStatisticsService but it is already bound");
        }
    }

    protected void unbindAaaMachineStatisticsService(AaaMachineStatisticsService machineStatisticsService) {
        log.info("unbindAaaMachineStatisticsService");
        if (this.machineStatisticsService == machineStatisticsService) {
            log.info("Unbinding AaaMachineStatisticsService");
            this.machineStatisticsService = null;
            log.info("Removing listener on AaaMachineStatisticsService");
            machineStatisticsService.removeListener(machineStatisticsEventListener);
        } else {
            log.warn("Trying to unbind AaaMachineStatisticsService but it is already unbound");
        }
    }

    @Activate
    public void activate() {
        log.info("Started AaaKafkaIntegration");
    }

    @Deactivate
    public void deactivate() {
        log.info("Stopped AaaKafkaIntegration");
    }

    private void handle(AuthenticationEvent event) {
        eventBusService.send(TOPIC, serialize(event));
    }

    private void handleStat(AuthenticationStatisticsEvent event) {
        eventBusService.send(AUTHENTICATION_STATISTICS_TOPIC, serializeStat(event));
        log.info("AuthenticationStatisticsEvent sent successfully");
    }

    private void handleMachineStat(AaaMachineStatisticsEvent machineStatEvent) {
        eventBusService.send(AUTHENTICATION_STATISTICS_TOPIC, serializeMachineStat(machineStatEvent));
        log.info("MachineStatisticsEvent sent successfully");
    }

    private JsonNode serialize(AuthenticationEvent event) {
        String sn = deviceService.getPort(event.subject()).annotations().value(AnnotationKeys.PORT_NAME);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode authEvent = mapper.createObjectNode();
        authEvent.put(TIMESTAMP, Instant.now().toString());
        authEvent.put(DEVICE_ID, event.subject().deviceId().toString());
        authEvent.put(PORT_NUMBER, event.subject().port().toString());
        authEvent.put(SERIAL_NUMBER, sn);
        authEvent.put(AUTHENTICATION_STATE, event.type().toString());
        return authEvent;
    }

    private JsonNode serializeStat(AuthenticationStatisticsEvent event) {
        log.info("Serializing AuthenticationStatisticsEvent");
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode authMetricsEvent = mapper.createObjectNode();
        authMetricsEvent.put(TIMESTAMP, Instant.now().toString());
        authMetricsEvent.put(ACCEPT_RESPONSES_RX, event.subject().getAcceptResponsesRx());
        authMetricsEvent.put(REJECT_RESPONSES_RX, event.subject().getRejectResponsesRx());
        authMetricsEvent.put(CHALLENGE_RESPONSES_RX, event.subject().getChallengeResponsesRx());
        authMetricsEvent.put(ACCESS_REQUESTS_TX, event.subject().getAccessRequestsTx());
        authMetricsEvent.put(INVALID_VALIDATORS_RX, event.subject().getInvalidValidatorsRx());
        authMetricsEvent.put(UNKNOWN_TYPE_RX, event.subject().getUnknownTypeRx());
        authMetricsEvent.put(PENDING_REQUESTS, event.subject().getPendingRequests());
        authMetricsEvent.put(DROPPED_RESPONSES_RX, event.subject().getDroppedResponsesRx());
        authMetricsEvent.put(MALFORMED_RESPONSES_RX, event.subject().getMalformedResponsesRx());
        authMetricsEvent.put(UNKNOWN_SERVER_RX, event.subject().getUnknownServerRx());
        authMetricsEvent.put(REQUEST_RTT_MILLIS, event.subject().getRequestRttMilis());
        authMetricsEvent.put(REQUEST_RE_TX, event.subject().getRequestReTx());
        return authMetricsEvent;
    }

    private JsonNode serializeMachineStat(AaaMachineStatisticsEvent machineStatEvent) {
        log.info("Serializing AuthenticationStatisticsEvent");
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode machineStat = mapper.createObjectNode();
        AaaSupplicantMachineStats subject = machineStatEvent.subject();
        machineStat.put(SESSION_ID, subject.getSessionId());
        machineStat.put(SESSION_NAME, subject.getSessionName());
        machineStat.put(MAC_ADDRESS, subject.getSrcMacAddress());
        machineStat.put(SESSION_DURATION, subject.getSessionDuration());
        machineStat.put(EAPOL_TYPE, subject.getEapolType());
        machineStat.put(TOTAL_FRAMES_RX, subject.getTotalFramesReceived());
        machineStat.put(TOTAL_FRAMES_TX, subject.getTotalFramesSent());
        machineStat.put(TOTAL_PACKETS_RX, subject.getTotalFramesReceived());
        machineStat.put(TOTAL_PACKETS_TX, subject.getTotalFramesSent());
        machineStat.put(TOTAL_OCTETS_RX, subject.getTotalOctetRecieved());
        machineStat.put(TOTAL_OCTETS_TX, subject.getTotalOctetSent());
        machineStat.put(SESSION_TERMINATE_REASON, subject.getSessionTerminateReason());
        log.debug(SESSION_ID + " - " + subject.getSessionId());
        log.debug(SESSION_NAME + " - " + subject.getSessionName());
        log.debug(MAC_ADDRESS + " - " + subject.getSrcMacAddress());
        log.debug(SESSION_DURATION + " - " + subject.getSessionDuration());
        log.debug(EAPOL_TYPE + " - " + subject.getEapolType());
        log.debug(TOTAL_FRAMES_RX + " - " + subject.getTotalFramesReceived());
        log.debug(TOTAL_FRAMES_TX + " - " + subject.getTotalFramesSent());
        log.debug(TOTAL_PACKETS_RX + " - " + subject.getTotalFramesReceived());
        log.debug(TOTAL_PACKETS_TX + " - " + subject.getTotalFramesSent());
        log.debug(TOTAL_OCTETS_RX + " - " + subject.getTotalOctetRecieved());
        log.debug(TOTAL_OCTETS_TX + " - " + subject.getTotalOctetSent());
        log.debug(SESSION_TERMINATE_REASON + " - " + subject.getSessionTerminateReason());
        return machineStat;
    }

    private class InternalAuthenticationListener implements
            AuthenticationEventListener {
        @Override
        public void event(AuthenticationEvent authenticationEvent) {
            handle(authenticationEvent);
        }
    }

    private class InternalAuthenticationStatisticsListner implements
    AuthenticationStatisticsEventListener {
        @Override
        public void event(AuthenticationStatisticsEvent authenticationStatisticsEvent) {
            handleStat(authenticationStatisticsEvent);
        }
    }

    private class InternalAaaMachineStatisticsListner implements AaaMachineStatisticsEventListener {

        @Override
        public void event(AaaMachineStatisticsEvent machineStatEvent) {
            handleMachineStat(machineStatEvent);
        }
    }
}
