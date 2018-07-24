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
import org.opencord.aaa.AuthenticationEvent;
import org.opencord.aaa.AuthenticationEventListener;
import org.opencord.aaa.AuthenticationService;
import org.opencord.kafka.EventBusService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listens for AAA events and pushes them on a Kafka bus.
 */
@Component(immediate = true)
public class AaaKafkaIntegration {

    public Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected EventBusService eventBusService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected AuthenticationService authenticationService;

    private final AuthenticationEventListener listener = new InternalAuthenticationListener();

    private static final String TOPIC = "authentication.events";

    private static final String DEVICE_ID = "device_id";
    private static final String PORT_NUMBER = "port_number";
    private static final String AUTHENTICATION_STATE = "authentication_state";

    @Activate
    public void activate() {
        authenticationService.addListener(listener);
        log.info("Started");
    }

    @Deactivate
    public void deactivate() {
        authenticationService.removeListener(listener);
        log.info("Stopped");
    }

    private void handle(AuthenticationEvent event) {
        eventBusService.send(TOPIC, serialize(event));
    }

    private JsonNode serialize(AuthenticationEvent event) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode authEvent = mapper.createObjectNode();
        authEvent.put(DEVICE_ID, event.subject().deviceId().toString());
        authEvent.put(PORT_NUMBER, event.subject().port().toString());
        authEvent.put(AUTHENTICATION_STATE, event.type().toString());
        return authEvent;
    }

    private class InternalAuthenticationListener implements
            AuthenticationEventListener {

        @Override
        public void event(AuthenticationEvent authenticationEvent) {
            handle(authenticationEvent);
        }
    }
}
