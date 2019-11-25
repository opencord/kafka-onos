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
import org.onosproject.net.AnnotationKeys;
import org.onosproject.net.device.DeviceService;
import org.opencord.aaa.AuthenticationEvent;
import org.opencord.aaa.AuthenticationEventListener;
import org.opencord.aaa.AuthenticationService;
import org.opencord.aaa.AuthenticationStatisticsEvent;
import org.opencord.aaa.AuthenticationStatisticsEventListener;
import org.opencord.aaa.AuthenticationStatisticsService;
import org.opencord.kafka.EventBusService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;

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

    private final AuthenticationEventListener listener = new InternalAuthenticationListener();
    private final AuthenticationStatisticsEventListener authenticationStatisticsEventListener =
             new InternalAuthenticationStatisticsListner();

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
        log.trace("AuthenticationStatisticsEvent sent successfully");
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
        log.trace("Serializing AuthenticationStatisticsEvent");
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
}