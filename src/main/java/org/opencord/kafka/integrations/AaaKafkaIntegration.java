/*
 * Copyright 2018-2023 Open Networking Foundation (ONF) and the ONF Contributors
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
import org.onosproject.net.AnnotationKeys;
import org.onosproject.net.device.DeviceService;
import org.opencord.aaa.AuthenticationEvent;
import org.opencord.aaa.AuthenticationEventListener;
import org.opencord.aaa.AuthenticationService;
import org.opencord.aaa.AuthenticationStatisticsEvent;
import org.opencord.aaa.AuthenticationStatisticsEventListener;
import org.opencord.aaa.AuthenticationStatisticsService;
import org.opencord.aaa.RadiusOperationalStatusEvent;
import org.opencord.aaa.RadiusOperationalStatusEventListener;
import org.opencord.aaa.RadiusOperationalStatusService;
import org.opencord.kafka.EventBusService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.opencord.aaa.AaaMachineStatisticsEvent;
import org.opencord.aaa.AaaMachineStatisticsEventListener;
import org.opencord.aaa.AaaMachineStatisticsService;
import org.opencord.aaa.AaaSupplicantMachineStats;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Listens for AAA events and pushes them on a Kafka bus.
 */
@Component(immediate = true)
public class AaaKafkaIntegration extends AbstractKafkaIntegration {

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected EventBusService eventBusService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected DeviceService deviceService;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL,
            policy = ReferencePolicy.DYNAMIC,
            bind = "bindAuthenticationService",
            unbind = "unbindAuthenticationService")
    protected volatile AuthenticationService ignore;
    private final AtomicReference<AuthenticationService> authServiceRef = new AtomicReference<>();
    @Reference(cardinality = ReferenceCardinality.OPTIONAL,
            policy = ReferencePolicy.DYNAMIC,
            bind = "bindAuthenticationStatService",
            unbind = "unbindAuthenticationStatService")
    protected volatile AuthenticationStatisticsService ignore2;
    private final AtomicReference<AuthenticationStatisticsService> authStatServiceRef = new AtomicReference<>();

    @Reference(cardinality = ReferenceCardinality.OPTIONAL,
            policy = ReferencePolicy.DYNAMIC,
            bind = "bindRadiusOperationalStatusService",
            unbind = "unbindRadiusOperationalStatusService")
    protected volatile RadiusOperationalStatusService ignore3;
    protected final AtomicReference<RadiusOperationalStatusService> radiusOperationalStatusServiceRef
            = new AtomicReference<>();

    @Reference(cardinality = ReferenceCardinality.OPTIONAL,
            policy = ReferencePolicy.DYNAMIC,
            bind = "bindAaaMachineStatisticsService",
            unbind = "unbindAaaMachineStatisticsService")
    protected volatile AaaMachineStatisticsService ignore4;
    protected final AtomicReference<AaaMachineStatisticsService> machineStatisticsServiceRef = new AtomicReference<>();

    private final AuthenticationEventListener listener = new InternalAuthenticationListener();
    private final AuthenticationStatisticsEventListener authenticationStatisticsEventListener =
            new InternalAuthenticationStatisticsListner();
    private final RadiusOperationalStatusEventListener radiusOperationalStatusEventListener =
            new InternalRadiusOperationalStatusEventListener();
    private final AaaMachineStatisticsEventListener machineStatisticsEventListener =
            new InternalAaaMachineStatisticsListener();

    // topics
    protected static final String TOPIC = "authentication.events";
    protected static final String AUTHENTICATION_STATISTICS_TOPIC = "onos.aaa.stats.kpis";
    protected static final String RADIUS_OPERATION_STATUS_TOPIC = "radiusOperationalStatus.events";
    // auth event params
    private static final String TIMESTAMP = "timestamp";
    private static final String DEVICE_ID = "deviceId";
    private static final String PORT_NUMBER = "portNumber";
    private static final String SERIAL_NUMBER = "serialNumber";
    private static final String AUTHENTICATION_STATE = "authenticationState";

    // auth stats event params
    private static final String RADIUS_ACCEPT_RESPONSES_RX = "radiusAccessAcceptRx";
    private static final String RADIUS_REJECT_RESPONSES_RX = "radiusRejectResponsesRx";
    private static final String RADIUS_CHALLENGE_RESPONSES_RX = "radiusAccessChallengeRx";
    private static final String RADIUS_ACCESS_REQUESTS_TX = "radiusAccessRequestTx";
    private static final String RADIUS_ACCESS_REQUESTS_IDENTITY_TX = "radiusAccessRequestIdentityTx";
    private static final String RADIUS_ACCESS_REQUESTS_CHALLENGE_TX = "radiusAccessRequestChallengeTx";
    private static final String RADIUS_PENDING_REQUESTS = "radiusPendingRequests";
    private static final String TIMED_OUT_PACKETS = "timedOutPackets";
    private static final String UNKNOWN_TYPE_RX = "unknownTypeRx";
    private static final String INVALID_VALIDATORS_RX = "invalidValidatorsRx";
    private static final String DROPPED_RESPONSES_RX = "droppedResponsesRx";
    private static final String MALFORMED_RESPONSES_RX = "malformedResponsesRx";
    private static final String UNKNOWN_SERVER_RX = "unknownServerRx";
    private static final String REQUEST_RTT_MILLIS = "requestRttMillis";
    private static final String REQUEST_RE_TX = "requestReTx";
    private static final String NUM_SESSIONS_EXPIRED = "numSessionsExpired";
    private static final String EAPOL_LOGOFF_RX = "eapolLogoffRx";
    private static final String EAPOL_AUTH_SUCCESS_TX = "eapolAuthSuccessTx";
    private static final String EAPOL_AUTH_FAILURE_TX = "eapolAuthFailureTrans";
    private static final String EAPOL_START_REQ_RX = "eapolStartRequestRx";
    private static final String EAPOL_MD5_CHALLENGE_RESP_RX = "eapolMd5ChallengeResponseRx";
    private static final String EAPOL_TLS_CHALLENGE_RESP = "eapolTlsRespChallenge";
    private static final String EAPOL_TRANS_RESP_NOT_NAK = "eapolTransRespNotNak";
    private static final String EAPOL_CHALLENGE_REQ_TX = "eapolChallengeRequestTx";
    private static final String EAPOL_ID_RESP_FRAMES_RX = "eapolIdentityResponseRx";
    private static final String EAPOL_ID_MSG_RESP_TX = "eapolIdentityMsgResponseTx";
    private static final String EAPOL_FRAMES_TX = "eapolFramesTx";
    private static final String AUTH_STATE_IDLE = "authStateIdle";
    private static final String EAPOL_ID_REQUEST_FRAMES_TX = "eapolIdentityRequestTx";
    private static final String EAPOL_REQUEST_FRAMES_TX = "eapolRequestFramesTx"; //TODO check
    private static final String INVALID_PKT_TYPE = "invalidPktType";
    private static final String INVALID_BODY_LENGTH = "invalidBodyLength";
    private static final String EAPOL_VALID_FRAMES_RX = "eapolValidFramesRx";
    private static final String EAPOL_PENDING_REQUESTS = "eapolPendingRequests";

    private static final String OPERATIONAL_STATUS = "radiusOperationalStatus";

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

    protected void bindAuthenticationService(AuthenticationService incomingService) {
        bindAndAddListener(incomingService, authServiceRef, listener);
    }

    protected void unbindAuthenticationService(AuthenticationService outgoingService) {
        unbindAndRemoveListener(outgoingService, authServiceRef, listener);
    }

    protected void bindAuthenticationStatService(AuthenticationStatisticsService incomingService) {
        bindAndAddListener(incomingService, authStatServiceRef, authenticationStatisticsEventListener);
    }

    protected void unbindAuthenticationStatService(AuthenticationStatisticsService outgoingService) {
        unbindAndRemoveListener(outgoingService, authStatServiceRef, authenticationStatisticsEventListener);
    }

    protected void bindRadiusOperationalStatusService(
            RadiusOperationalStatusService radiusOperationalStatusService) {
        bindAndAddListener(radiusOperationalStatusService, radiusOperationalStatusServiceRef,
                radiusOperationalStatusEventListener);
    }

    protected void unbindRadiusOperationalStatusService(RadiusOperationalStatusService radiusOperationalStatusService) {
        unbindAndRemoveListener(radiusOperationalStatusService, radiusOperationalStatusServiceRef,
                radiusOperationalStatusEventListener);
    }

    protected void bindAaaMachineStatisticsService(AaaMachineStatisticsService machineStatisticsService) {
        bindAndAddListener(machineStatisticsService, machineStatisticsServiceRef, machineStatisticsEventListener);
    }

    protected void unbindAaaMachineStatisticsService(AaaMachineStatisticsService machineStatisticsService) {
        unbindAndRemoveListener(machineStatisticsService, machineStatisticsServiceRef, machineStatisticsEventListener);
    }

    @Activate
    public void activate() {
        log.info("Started AaaKafkaIntegration");
    }

    @Deactivate
    public void deactivate() {
        unbindAuthenticationService(authServiceRef.get());
        unbindAuthenticationStatService(authStatServiceRef.get());
        log.info("Stopped AaaKafkaIntegration");
    }

    private void handle(AuthenticationEvent event) {
        eventBusService.send(TOPIC, serialize(event));
    }

    private void handleStat(AuthenticationStatisticsEvent event) {
        eventBusService.send(AUTHENTICATION_STATISTICS_TOPIC, serializeStat(event));
        if (log.isTraceEnabled()) {
            log.trace("AuthenticationStatisticsEvent sent successfully");
        }
    }

    private void handleOperationalStatus(RadiusOperationalStatusEvent event) {
        eventBusService.send(RADIUS_OPERATION_STATUS_TOPIC, serializeOperationalStatus(event));
        if (log.isTraceEnabled()) {
            log.trace("RadiusOperationalStatusEvent sent successfully");
        }
    }

    private void handleMachineStat(AaaMachineStatisticsEvent machineStatEvent) {
        eventBusService.send(AUTHENTICATION_STATISTICS_TOPIC, serializeMachineStat(machineStatEvent));
        if (log.isTraceEnabled()) {
            log.trace("MachineStatisticsEvent sent successfully");
        }
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
        if (log.isTraceEnabled()) {
            log.trace("Serializing AuthenticationStatisticsEvent");
        }
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode authMetricsEvent = mapper.createObjectNode();
        authMetricsEvent.put(TIMESTAMP, Instant.now().toString());
        authMetricsEvent.put(RADIUS_ACCEPT_RESPONSES_RX, event.subject().getRadiusAcceptResponsesRx());
        authMetricsEvent.put(RADIUS_REJECT_RESPONSES_RX, event.subject().getRadiusRejectResponsesRx());
        authMetricsEvent.put(RADIUS_CHALLENGE_RESPONSES_RX, event.subject().getRadiusChallengeResponsesRx());
        authMetricsEvent.put(RADIUS_ACCESS_REQUESTS_IDENTITY_TX, event.subject().getRadiusReqIdTx());
        authMetricsEvent.put(RADIUS_ACCESS_REQUESTS_CHALLENGE_TX, event.subject().getRadiusReqChallengeTx());
        authMetricsEvent.put(RADIUS_ACCESS_REQUESTS_TX, event.subject().getRadiusAccessRequestsTx());
        authMetricsEvent.put(INVALID_VALIDATORS_RX, event.subject().getInvalidValidatorsRx());
        authMetricsEvent.put(UNKNOWN_TYPE_RX, event.subject().getUnknownTypeRx());
        authMetricsEvent.put(RADIUS_PENDING_REQUESTS, event.subject().getRadiusPendingRequests());
        authMetricsEvent.put(DROPPED_RESPONSES_RX, event.subject().getDroppedResponsesRx());
        authMetricsEvent.put(MALFORMED_RESPONSES_RX, event.subject().getMalformedResponsesRx());
        authMetricsEvent.put(UNKNOWN_SERVER_RX, event.subject().getUnknownServerRx());
        authMetricsEvent.put(REQUEST_RTT_MILLIS, event.subject().getRequestRttMilis());
        authMetricsEvent.put(REQUEST_RE_TX, event.subject().getRequestReTx());
        authMetricsEvent.put(TIMED_OUT_PACKETS, event.subject().getTimedOutPackets());
        authMetricsEvent.put(EAPOL_LOGOFF_RX, event.subject().getEapolLogoffRx());
        authMetricsEvent.put(EAPOL_ID_MSG_RESP_TX, event.subject().getEapolResIdentityMsgTrans());
        authMetricsEvent.put(EAPOL_AUTH_SUCCESS_TX, event.subject().getEapolAuthSuccessTx());
        authMetricsEvent.put(EAPOL_AUTH_FAILURE_TX, event.subject().getEapolAuthFailureTx());
        authMetricsEvent.put(EAPOL_START_REQ_RX, event.subject().getEapolStartReqRx());
        authMetricsEvent.put(EAPOL_CHALLENGE_REQ_TX, event.subject().getEapolChallengeReqTx());
        authMetricsEvent.put(EAPOL_TRANS_RESP_NOT_NAK, event.subject().getEapolTransRespNotNak());
        authMetricsEvent.put(EAPOL_FRAMES_TX, event.subject().getEapolFramesTx());
        authMetricsEvent.put(AUTH_STATE_IDLE, event.subject().getAuthStateIdle());
        authMetricsEvent.put(EAPOL_ID_REQUEST_FRAMES_TX, event.subject().getEapolIdRequestFramesTx());
        authMetricsEvent.put(EAPOL_REQUEST_FRAMES_TX, event.subject().getEapolReqFramesTx());
        authMetricsEvent.put(INVALID_PKT_TYPE, event.subject().getInvalidPktType());
        authMetricsEvent.put(INVALID_BODY_LENGTH, event.subject().getInvalidBodyLength());
        authMetricsEvent.put(EAPOL_VALID_FRAMES_RX, event.subject().getEapolValidFramesRx());
        authMetricsEvent.put(EAPOL_PENDING_REQUESTS, event.subject().getEapolPendingReq());
        authMetricsEvent.put(EAPOL_ID_RESP_FRAMES_RX, event.subject().getEapolattrIdentity());
        return authMetricsEvent;
    }

    private JsonNode serializeOperationalStatus(RadiusOperationalStatusEvent event) {
        if (log.isTraceEnabled()) {
            log.trace("Serializing RadiusOperationalStatusEvent: {}", event.subject());
        }
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode authMetricsEvent = mapper.createObjectNode();
        authMetricsEvent.put(TIMESTAMP, Instant.now().toString());
        authMetricsEvent.put(OPERATIONAL_STATUS, event.subject());
        return authMetricsEvent;
    }

    private JsonNode serializeMachineStat(AaaMachineStatisticsEvent machineStatEvent) {
        if (log.isTraceEnabled()) {
            log.trace("Serializing AuthenticationStatisticsEvent");
        }
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode machineStat = mapper.createObjectNode();
        AaaSupplicantMachineStats subject = machineStatEvent.subject();
        machineStat.put(TIMESTAMP, Instant.now().toString());
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

    private class InternalRadiusOperationalStatusEventListener implements
           RadiusOperationalStatusEventListener {
        @Override
        public void event(RadiusOperationalStatusEvent radiusOperationalStatusEvent) {
            handleOperationalStatus(radiusOperationalStatusEvent);
        }
    }

    private class InternalAaaMachineStatisticsListener implements AaaMachineStatisticsEventListener {

        @Override
        public void event(AaaMachineStatisticsEvent machineStatEvent) {
            handleMachineStat(machineStatEvent);
        }
    }
}
