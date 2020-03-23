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
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.onlab.packet.DHCP;
import org.onlab.packet.IpAddress;
import org.onlab.packet.MacAddress;
import org.onosproject.cluster.ClusterServiceAdapter;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.Port;
import org.opencord.dhcpl2relay.DhcpAllocationInfo;
import org.opencord.dhcpl2relay.DhcpL2RelayEvent;
import org.opencord.dhcpl2relay.DhcpL2RelayListener;
import org.opencord.dhcpl2relay.DhcpL2RelayService;
import org.opencord.kafka.EventBusService;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.opencord.kafka.integrations.MockDeviceService.DEVICE_ID_1;

class DhcpL2RelayKafkaIntegrationTest {

    private static final MacAddress OLT_MAC_ADDRESS = MacAddress.valueOf("01:02:03:04:05:06");
    private DhcpL2RelayKafkaIntegration dhcpL2RelayKafkaIntegration;
    private DhcpL2RelayListener dhcpL2RelayListener;
    private static final String TOPIC = "dhcp.events";
    private static final String DHCP_STATS_TOPIC = "onos.dhcp.stats.kpis";

    @BeforeEach
    void setUp() {
        dhcpL2RelayKafkaIntegration = new DhcpL2RelayKafkaIntegration();
        dhcpL2RelayKafkaIntegration.clusterService = new ClusterServiceAdapter();
        dhcpL2RelayKafkaIntegration.deviceService = new MockDeviceService();
        dhcpL2RelayKafkaIntegration.eventBusService = new MockEventBusService();
        dhcpL2RelayKafkaIntegration.ignore = new MockDhcpL2RelayService();
        dhcpL2RelayKafkaIntegration.bindDhcpL2RelayService(dhcpL2RelayKafkaIntegration.ignore);
        dhcpL2RelayKafkaIntegration.activate();
    }

    @AfterEach
    void tearDown() {
        dhcpL2RelayKafkaIntegration.deactivate();
    }

    @Test
    void testDhcpL2RelayStatsUpdate() {
        Port port = new org.opencord.kafka.integrations.MockDeviceService.MockPort();
        ConnectPoint cp = new ConnectPoint(DEVICE_ID_1, port.number());
        Map.Entry<String, AtomicLong> entryCounter = Maps.immutableEntry("DHCPREQUEST", new AtomicLong(1));
        String dhcpCountersTopic = "DHCPREQUEST";
        String subscriber = null;
        DhcpAllocationInfo allocationInfo = new DhcpAllocationInfo(
                new ConnectPoint(DEVICE_ID_1, port.number()),
                DHCP.MsgType.DHCPREQUEST, "TWSH00008084", OLT_MAC_ADDRESS,
                IpAddress.valueOf("127.0.0.1"));
        DhcpL2RelayEvent event = new DhcpL2RelayEvent(DhcpL2RelayEvent.Type.STATS_UPDATE,
                allocationInfo, cp, entryCounter, dhcpCountersTopic, subscriber);
        dhcpL2RelayListener.event(event);
        Assert.assertNotEquals(MockEventBusService.dhcpStats, 0);
    }

    @Test
    void testDhcpL2RelayStatsSubscriberUpdate() {
        Port port = new org.opencord.kafka.integrations.MockDeviceService.MockPort();
        ConnectPoint cp = new ConnectPoint(DEVICE_ID_1, port.number());
        Map.Entry<String, AtomicLong> entryCounter = Maps.immutableEntry("DHCPREQUEST", new AtomicLong(1));
        String dhcpCountersTopic = "DHCPREQUEST";
        String subscriber = "TWSH00008084";
        DhcpAllocationInfo allocationInfo = new DhcpAllocationInfo(
                new ConnectPoint(DEVICE_ID_1, port.number()),
                DHCP.MsgType.DHCPREQUEST, "TWSH00008084", OLT_MAC_ADDRESS,
                IpAddress.valueOf("127.0.0.1"));
        DhcpL2RelayEvent event = new DhcpL2RelayEvent(DhcpL2RelayEvent.Type.STATS_UPDATE,
                allocationInfo, cp, entryCounter, dhcpCountersTopic, subscriber);
        dhcpL2RelayListener.event(event);
        Assert.assertNotEquals(MockEventBusService.dhcpStats, 0);
    }

    @Test
    void testDhcpL2RelayUpdate() {
        Port port = new org.opencord.kafka.integrations.MockDeviceService.MockPort();
        ConnectPoint cp = new ConnectPoint(DEVICE_ID_1, port.number());
        Map.Entry<String, AtomicLong> entryCounter = Maps.immutableEntry("DHCPREQUEST", new AtomicLong(1));
        String dhcpCountersTopic = "DHCPREQUEST";
        String subscriber = null;
        DhcpAllocationInfo allocationInfo = new DhcpAllocationInfo(
                new ConnectPoint(DEVICE_ID_1, port.number()),
                DHCP.MsgType.DHCPREQUEST, "TWSH00008084", OLT_MAC_ADDRESS,
                IpAddress.valueOf("127.0.0.1"));
        DhcpL2RelayEvent event = new DhcpL2RelayEvent(DhcpL2RelayEvent.Type.UPDATED,
                allocationInfo, cp, entryCounter, dhcpCountersTopic, subscriber);
        dhcpL2RelayListener.event(event);
        Assert.assertNotEquals(MockEventBusService.dhcpEvents, 0);
    }

    private static class MockEventBusService implements EventBusService {
        static int dhcpStats = 0;
        static int dhcpEvents = 0;

        @Override
        public void send(String topic, JsonNode data) {
            switch (topic) {
                case DHCP_STATS_TOPIC:
                    dhcpStats++;
                    break;
                case TOPIC:
                    dhcpEvents++;
                    break;
                default:
                    break;
            }
        }
    }

    private class MockDhcpL2RelayService implements DhcpL2RelayService {
        @Override
        public void addListener(DhcpL2RelayListener listener) {
            dhcpL2RelayListener = listener;
        }

        @Override
        public void removeListener(DhcpL2RelayListener listener) {

        }
    }
}