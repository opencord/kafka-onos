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
import org.onosproject.cluster.ClusterService;
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
    protected ClusterService clusterService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected DeviceService deviceService;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policy = ReferencePolicy.DYNAMIC,
            bind = "bindDhcpL2RelayService",
            unbind = "unbindDhcpL2RelayService")
    protected DhcpL2RelayService dhcpL2RelayService;

    private final DhcpL2RelayListener listener = new InternalDhcpL2RelayListener();

    // topics
    private static final String TOPIC = "dhcp.events";
    private static final String DHCP_STATS_TOPIC = "onos.dhcp.stats.kpis";

    private static final String TIMESTAMP = "timestamp";
    private static final String DEVICE_ID = "deviceId";
    private static final String PORT_NUMBER = "portNumber";
    private static final String SERIAL_NUMBER = "serialNumber";
    private static final String TYPE = "type";
    private static final String MESSAGE_TYPE = "messageType";
    private static final String MAC_ADDRESS = "macAddress";
    private static final String IP_ADDRESS = "ipAddress";

    // dhcp stats event params
    static final String CONNECT_POINT = "connectPoint";
    static final String INSTANCE_ID = "instance_id";
    static final String METRICS = "metrics";
    static final String SUBSCRIBER_ID = "subscriberId";
    static final String SUBSCRIBER_INFO = "subscriberInfo";
    static final String TS = "ts";
    static final String TITLE = "title";

    static final String GLOBAL_STATS_TITLE = "DHCP_L2_Relay_stats";
    static final String PER_SUBSCRIBER_STATS_TITLE = "DHCP_L2_Relay_stats_Per_Subscriber";

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
        switch (event.type()) {
            case STATS_UPDATE:
                // pushes the stats based on the received event (per subscriber or global) on a Kafka bus
                if (event.getSubscriberId() != null && event.subject() != null) {
                    eventBusService.send(DHCP_STATS_TOPIC, serializeStat(event, PER_SUBSCRIBER_STATS_TITLE));
                } else {
                    eventBusService.send(DHCP_STATS_TOPIC, serializeStat(event, GLOBAL_STATS_TITLE));
                }
                log.info("Writing to kafka topic:{}, type:{}", DHCP_STATS_TOPIC,
                        DhcpL2RelayEvent.Type.STATS_UPDATE.toString());
                break;
            default:
                eventBusService.send(TOPIC, serialize(event));
                log.info("Writing to kafka topic:{}, type:{}", TOPIC, event.type().toString());
                break;
        }
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

    /**
     * Returns a Json object that represents the DHCP L2 Relay stats.
     *
     * @param event DHCP L2 Relay event used for stats.
     * @param title Describes the type of the received stats event (per subscriber or global).
     */
    private JsonNode serializeStat(DhcpL2RelayEvent event, String title) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode statsEvent = mapper.createObjectNode();
        Long ts = Instant.now().getEpochSecond();

        // metrics for global and per subscriber stats
        ObjectNode metrics = mapper.createObjectNode();
        metrics.put(event.getCountersEntry().getKey(), event.getCountersEntry().getValue().longValue());

        statsEvent.put(INSTANCE_ID, clusterService.getLocalNode().id().toString());
        statsEvent.put(TITLE, title);
        statsEvent.put(TS, ts);
        statsEvent.put(METRICS, metrics);

        // specific metrics for per subscriber stats
        if (event.getSubscriberId() != null && event.subject() != null) {
            String sn = deviceService.getDevice(event.subject().location().deviceId()).serialNumber();
            ObjectNode subscriberInfo = mapper.createObjectNode();

            statsEvent.put(SERIAL_NUMBER, sn);
            subscriberInfo.put(SUBSCRIBER_ID, event.getSubscriberId());
            subscriberInfo.put(CONNECT_POINT, event.subject().location().toString());
            subscriberInfo.put(MAC_ADDRESS, event.subject().macAddress().toString());

            statsEvent.put(SUBSCRIBER_INFO, subscriberInfo);
        }

        return statsEvent;
    }

    private class InternalDhcpL2RelayListener implements
            DhcpL2RelayListener {

        @Override
        public void event(DhcpL2RelayEvent dhcpL2RelayEvent) {
            handle(dhcpL2RelayEvent);
        }
    }
}
