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
import org.opencord.dhcpl2relay.DhcpAllocationInfo;
import org.opencord.dhcpl2relay.DhcpL2RelayEvent;
import org.opencord.dhcpl2relay.DhcpL2RelayListener;
import org.opencord.dhcpl2relay.DhcpL2RelayService;
import org.opencord.kafka.EventBusService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Listens for DHCP L2 relay events and pushes them on a Kafka bus.
 */
@Component(immediate = true)
public class DhcpL2RelayKafkaIntegration {

    public Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected EventBusService eventBusService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected DeviceService deviceService;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policy = ReferencePolicy.DYNAMIC,
            bind = "bindDhcpL2RelayService",
            unbind = "unbindDhcpL2RelayService")
    protected DhcpL2RelayService dhcpL2RelayService;

    private final DhcpL2RelayListener listener = new InternalDhcpL2RelayListener();

    private static final String TOPIC = "dhcp.events";

    private static final String TIMESTAMP = "timestamp";
    private static final String DEVICE_ID = "deviceId";
    private static final String PORT_NUMBER = "portNumber";
    private static final String SERIAL_NUMBER = "serialNumber";
    private static final String TYPE = "type";
    private static final String MESSAGE_TYPE = "messageType";
    private static final String MAC_ADDRESS = "macAddress";
    private static final String IP_ADDRESS = "ipAddress";

    protected void bindDhcpL2RelayService(DhcpL2RelayService dhcpL2RelayService) {
        if (this.dhcpL2RelayService == null) {
            log.info("Binding DhcpL2RelayService");
            this.dhcpL2RelayService = dhcpL2RelayService;
            log.info("Adding listener on DhcpL2RelayService");
            dhcpL2RelayService.addListener(listener);
        } else {
            log.warn("Trying to bind DhcpL2RelayService but it is already bound");
        }
    }

    protected void unbindDhcpL2RelayService(DhcpL2RelayService dhcpL2RelayService) {
        if (this.dhcpL2RelayService == dhcpL2RelayService) {
            log.info("Unbinding DhcpL2RelayService");
            this.dhcpL2RelayService = null;
            log.info("Removing listener on DhcpL2RelayService");
            dhcpL2RelayService.removeListener(listener);
        } else {
            log.warn("Trying to unbind DhcpL2RelayService but it is already unbound");
        }
    }

    @Activate
    public void activate() {
        log.info("Started DhcpL2RelayKafkaIntegration");
    }

    @Deactivate
    public void deactivate() {
        log.info("Stopped DhcpL2RelayKafkaIntegration");
    }

    private void handle(DhcpL2RelayEvent event) {
        eventBusService.send(TOPIC, serialize(event));
    }

    private JsonNode serialize(DhcpL2RelayEvent event) {

        String sn = deviceService.getPort(event.subject().location()).annotations().value(AnnotationKeys.PORT_NAME);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode dhcpEvent = mapper.createObjectNode();
        DhcpAllocationInfo allocationInfo = event.subject();
        dhcpEvent.put(TYPE, event.type().toString());
        dhcpEvent.put(TIMESTAMP, Instant.now().toString());
        dhcpEvent.put(DEVICE_ID, event.connectPoint().deviceId().toString());
        dhcpEvent.put(PORT_NUMBER, event.connectPoint().port().toString());
        dhcpEvent.put(SERIAL_NUMBER, sn);
        dhcpEvent.put(MESSAGE_TYPE, allocationInfo.type().toString());
        dhcpEvent.put(MAC_ADDRESS, allocationInfo.macAddress().toString());
        dhcpEvent.put(IP_ADDRESS, allocationInfo.ipAddress().toString());
        return dhcpEvent;
    }

    private class InternalDhcpL2RelayListener implements
            DhcpL2RelayListener {

        @Override
        public void event(DhcpL2RelayEvent dhcpL2RelayEvent) {
            handle(dhcpL2RelayEvent);
        }
    }
}
