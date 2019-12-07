/*
 * Copyright 2019-present Open Networking Foundation
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
import org.opencord.bng.PppoeBngControlHandler;
import org.opencord.bng.PppoeEvent;
import org.opencord.bng.PppoeEventListener;
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
 * Listens for PPPoE handler events from the BNG app and pushes them on a Kafka
 * bus.
 */
@Component(immediate = true)
public class BngPppoeKafkaIntegration extends AbstractKafkaIntegration {

    private static final String TOPIC_PPPOE = "bng.pppoe";
    private static final String TIMESTAMP = "timestamp";
    private static final String EVENT_TYPE = "eventType";
    private static final String OLT_DEVICE_ID = "deviceId";
    private static final String OLT_PORT_NUMBER = "portNumber";
    private static final String MAC_ADDRESS = "macAddress";
    private static final String IP_ADDRESS = "ipAddress";
    private static final String SERIAL_NUMBER = "serialNumber";
    private static final String SESSION_ID = "sessionId";

    private final PppoeEventListener pppoeEventListener = new InternalPppoeListener();

    private final AtomicReference<PppoeBngControlHandler> pppoeBngControlRef = new AtomicReference<>();

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected EventBusService eventBusService;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL,
            policy = ReferencePolicy.DYNAMIC,
            bind = "bindPppoeBngControl",
            unbind = "unbindPppoeBngControl")
    protected volatile PppoeBngControlHandler pppoeBngControlHandler;

    protected void bindPppoeBngControl(PppoeBngControlHandler incomingService) {
        bindAndAddListener(incomingService, pppoeBngControlRef, pppoeEventListener);
    }

    protected void unbindPppoeBngControl(PppoeBngControlHandler outgoingService) {
        unbindAndRemoveListener(outgoingService, pppoeBngControlRef, pppoeEventListener);
    }

    @Activate
    public void activate() {
        log.info("Started PppoeKafkaIntegration");
    }

    @Deactivate
    public void deactivate() {
        unbindPppoeBngControl(pppoeBngControlRef.get());
        log.info("Stopped PppoeKafkaIntegration");
    }

    private JsonNode serializePppoeEvent(PppoeEvent event) {
        // Serialize PPPoE event in a JSON node
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode pppoeEvent = mapper.createObjectNode();
        pppoeEvent.put(TIMESTAMP, Instant.now().toString());
        pppoeEvent.put(EVENT_TYPE, event.type().toString());
        pppoeEvent.put(OLT_DEVICE_ID, event.subject().getOltConnectPoint().deviceId().toString());
        pppoeEvent.put(OLT_PORT_NUMBER, event.subject().getOltConnectPoint().port().toString());
        pppoeEvent.put(MAC_ADDRESS, event.subject().getMacAddress().toString());
        pppoeEvent.put(IP_ADDRESS, event.subject().getIpAddress().toString());
        pppoeEvent.put(SERIAL_NUMBER, event.subject().getOnuSerialNumber());
        pppoeEvent.put(SESSION_ID, event.subject().getSessionId() == 0 ?
                "" : String.valueOf(event.subject().getSessionId()));
        return pppoeEvent;
    }

    private class InternalPppoeListener implements PppoeEventListener {

        @Override
        public void event(PppoeEvent event) {
            eventBusService.send(TOPIC_PPPOE, serializePppoeEvent(event));
        }
    }
}
