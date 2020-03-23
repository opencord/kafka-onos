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
import org.onosproject.net.Device;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Port;
import org.onosproject.net.device.DeviceService;
import org.opencord.kafka.EventBusService;
import org.opencord.olt.AccessDeviceEvent;
import org.opencord.olt.AccessDeviceListener;
import org.opencord.olt.AccessDeviceService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Listens for access device events and pushes them on a Kafka bus.
 */
@Component(immediate = true)
public class AccessDeviceKafkaIntegration extends AbstractKafkaIntegration {

    public Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected EventBusService eventBusService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected DeviceService deviceService;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL,
            policy = ReferencePolicy.DYNAMIC,
            bind = "bindAccessDeviceService",
            unbind = "unbindAccessDeviceService")
    protected volatile AccessDeviceService accessDeviceService;
    private final AtomicReference<AccessDeviceService> accessDeviceServiceRef = new AtomicReference<>();

    private final AccessDeviceListener listener = new InternalAccessDeviceListener();

    protected static final String TOPIC = "onu.events";

    // event fields
    private static final String STATUS = "status";
    private static final String SERIAL_NUMBER = "serialNumber";
    private static final String PORT_NUMBER = "portNumber";  // uni port
    private static final String DEVICE_ID = "deviceId";  // OLT OpenFlow Id
    private static final String TIMESTAMP = "timestamp";
    private static final String OLT_SERIAL_NUMBER = "oltSerialNumber"; // OLT Serial Number

    // statuses
    private static final String ACTIVATED = "activated";
    private static final String DISABLED = "disabled";

    protected void bindAccessDeviceService(AccessDeviceService incomingService) {
        bindAndAddListener(incomingService, accessDeviceServiceRef, listener);
    }

    protected void unbindAccessDeviceService(AccessDeviceService outgoingService) {
        unbindAndRemoveListener(outgoingService, accessDeviceServiceRef, listener);
    }

    @Activate
    public void activate() {
        log.info("Started AccessDeviceKafkaIntegration");
    }

    @Deactivate
    public void deactivate() {
        unbindAccessDeviceService(accessDeviceServiceRef.get());
        log.info("Stopped AccessDeviceKafkaIntegration");
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
        onuNode.put(PORT_NUMBER, port.number().toString());
        onuNode.put(DEVICE_ID, port.element().id().toString());

        Device d = deviceService.getDevice((DeviceId) port.element().id());
        if (d != null) {
            onuNode.put(OLT_SERIAL_NUMBER, d.serialNumber());
        }

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
