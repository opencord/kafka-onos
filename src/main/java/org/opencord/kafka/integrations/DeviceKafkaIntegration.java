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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.onosproject.net.DeviceId;
import org.onosproject.net.device.DeviceEvent;
import org.onosproject.net.device.DeviceListener;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.device.PortStatistics;
import org.onosproject.net.Port;
import org.opencord.kafka.EventBusService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;


/**
 * Listens for access device events and pushes them on a Kafka bus.
 */
@Component(immediate = true)
public class DeviceKafkaIntegration {

    public Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected EventBusService eventBusService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected DeviceService deviceService;

    private final DeviceListener listener = new InternalDeviceListener();

    private static final String TOPIC = "onos.kpis";
    private static final String PORT_EVENT_TOPIC = "onos.events.port";

    // event fields
    private static final String TIMESTAMP = "timestamp";
    private static final String DEVICE_ID = "deviceId";
    private static final String PORTS = "ports";
    private static final String PORT_ID = "portId";
    private static final String PKT_RX = "pktRx";
    private static final String PKT_TX = "pktTx";
    private static final String BYTES_RX = "bytesRx";
    private static final String BYTES_TX = "bytesTx";
    private static final String PKT_RX_DROP = "pktRxDrp";
    private static final String PKT_TX_DROP = "pktTxDrp";
    private static final String ENABLED = "enabled";
    private static final String SPEED = "speed";
    private static final String TYPE = "type";

    @Activate
    public void activate() {
        deviceService.addListener(listener);
        log.info("Started");
    }

    @Deactivate
    public void deactivate() {
        deviceService.removeListener(listener);
        log.info("Stopped");
    }

    private void handle(List<PortStatistics> stats, DeviceId deviceId) {
        eventBusService.send(TOPIC, serializeStats(stats, deviceId));
    }

    private void handlePortUpdate(Port port, DeviceId deviceId) {
        eventBusService.send(PORT_EVENT_TOPIC, serializePort(port, deviceId));
    }

    private JsonNode serializeStats(List<PortStatistics> stats, DeviceId deviceId) {

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode kpis = mapper.createObjectNode();
        ArrayNode ports = mapper.createArrayNode();

        for (Iterator<PortStatistics> i = stats.iterator(); i.hasNext();) {
            PortStatistics stat = i.next();

            ObjectNode port = mapper.createObjectNode();
            port.put(PORT_ID, stat.portNumber().toString());
            port.put(PKT_RX, stat.packetsReceived());
            port.put(PKT_TX, stat.packetsSent());
            port.put(BYTES_RX, stat.bytesReceived());
            port.put(BYTES_TX, stat.bytesSent());
            port.put(PKT_RX_DROP, stat.packetsRxDropped());
            port.put(PKT_TX_DROP, stat.packetsTxDropped());

            ports.add(port);
        }

        kpis.put(TIMESTAMP, Instant.now().toString());
        kpis.put(PORTS, ports);
        kpis.put(DEVICE_ID, deviceId.toString());

        return kpis;
    }

    private JsonNode serializePort(Port port, DeviceId deviceId) {

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode update = mapper.createObjectNode();

        update.put(TIMESTAMP, Instant.now().toString());
        update.put(DEVICE_ID, deviceId.toString());
        update.put(PORT_ID, port.number().toString());
        update.put(ENABLED, port.isEnabled());
        update.put(SPEED, port.portSpeed());
        update.put(TYPE, port.type().toString());

        return update;
    }

    private class InternalDeviceListener implements
            DeviceListener {

        @Override
        public void event(DeviceEvent deviceEvent) {
            final DeviceId deviceId;

            if (deviceEvent.subject().manufacturer().contains("VOLTHA")) {
                // TODO check the NNI port instead
                return;
            }

            log.trace("Got DeviceEvent: " + deviceEvent.type());
            switch (deviceEvent.type()) {
            case PORT_STATS_UPDATED:
                deviceId = deviceEvent.subject().id();
                final List<PortStatistics> stats = deviceService.getPortStatistics(deviceId);
                handle(stats, deviceId);
                break;
            case PORT_UPDATED:
                deviceId = deviceEvent.subject().id();
                final Port port = deviceEvent.port();
                handlePortUpdate(port, deviceId);
                break;
            default:
                break;
            }
        }
    }
}
