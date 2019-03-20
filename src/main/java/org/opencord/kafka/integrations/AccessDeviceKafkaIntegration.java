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
import org.onosproject.net.AnnotationKeys;
import org.onosproject.net.Port;
import org.opencord.kafka.EventBusService;
import org.opencord.olt.AccessDeviceEvent;
import org.opencord.olt.AccessDeviceListener;
import org.opencord.olt.AccessDeviceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Instant;

/**
 * Listens for access device events and pushes them on a Kafka bus.
 */
@Component(immediate = true)
public class AccessDeviceKafkaIntegration {

    public Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected EventBusService eventBusService;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            bind = "bindAccessDeviceService",
            unbind = "unbindAccessDeviceService")
    protected AccessDeviceService accessDeviceService;

    private final AccessDeviceListener listener = new InternalAccessDeviceListener();

    private static final String TOPIC = "onu.events";

    // event fields
    private static final String STATUS = "status";
    private static final String SERIAL_NUMBER = "serial_number";
    private static final String UNI_PORT_ID = "uni_port_id";
    private static final String OF_DPID = "of_dpid";
    private static final String TIMESTAMP = "timestamp";

    // statuses
    private static final String ACTIVATED = "activated";
    private static final String DISABLED = "disabled";

    protected void bindAccessDeviceService(AccessDeviceService accessDeviceService) {
        if (this.accessDeviceService == null) {
            log.info("Binding AccessDeviceService");
            this.accessDeviceService = accessDeviceService;
            log.info("Adding listener on AccessDeviceService");
            accessDeviceService.addListener(listener);
        } else {
            log.warn("Trying to bind AccessDeviceService but it is already bound");
        }
    }

    protected void unbindAccessDeviceService(AccessDeviceService accessDeviceService) {
        if (this.accessDeviceService == accessDeviceService) {
            log.info("Unbinding AccessDeviceService");
            this.accessDeviceService = null;
            log.info("Removing listener on AccessDeviceService");
            accessDeviceService.removeListener(listener);
        } else {
            log.warn("Trying to unbind AccessDeviceService but it is already unbound");
        }
    }

    @Activate
    public void activate() {
        log.info("Started");
    }

    @Deactivate
    public void deactivate() {
        log.info("Stopped");
    }

    private void handle(AccessDeviceEvent event, String status) {
        eventBusService.send(TOPIC, serialize(event, status));
    }

    private JsonNode serialize(AccessDeviceEvent event, String status) {
        Port port = event.port().get();
        String serialNumber = port.annotations().value(AnnotationKeys.PORT_NAME);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode onuNode = mapper.createObjectNode();
        onuNode.put(TIMESTAMP, Instant.now().toString());
        onuNode.put(STATUS, status);
        onuNode.put(SERIAL_NUMBER, serialNumber);
        onuNode.put(UNI_PORT_ID, port.number().toLong());
        onuNode.put(OF_DPID, port.element().id().toString());

        return onuNode;
    }

    private class InternalAccessDeviceListener implements
            AccessDeviceListener {

        @Override
        public void event(AccessDeviceEvent accessDeviceEvent) {
            log.info("Got AccessDeviceEvent: " + accessDeviceEvent.type());
            switch (accessDeviceEvent.type()) {
            case UNI_ADDED:
                handle(accessDeviceEvent, ACTIVATED);
                break;
            case UNI_REMOVED:
                handle(accessDeviceEvent, DISABLED);
            default:
                break;
            }
        }
    }
}
