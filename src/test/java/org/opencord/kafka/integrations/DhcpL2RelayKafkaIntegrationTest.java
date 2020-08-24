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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.onlab.packet.DHCP;
import org.onosproject.cluster.ClusterServiceAdapter;
import org.onosproject.net.ConnectPoint;
import org.opencord.dhcpl2relay.DhcpAllocationInfo;
import org.opencord.dhcpl2relay.DhcpL2RelayEvent;
import org.opencord.dhcpl2relay.DhcpL2RelayListener;
import org.opencord.dhcpl2relay.DhcpL2RelayService;
import org.opencord.kafka.EventBusService;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.opencord.kafka.integrations.MockDeviceService.DEVICE_ID_1;

/**
 * set of unit test cases for DhcpL2RelayKafkaIntegration.
 */
class DhcpL2RelayKafkaIntegrationTest extends KafkaIntegrationTestBase {

    private static final String DHCP_COUNTER_TOPIC = "DHCPREQUEST";
    private DhcpL2RelayKafkaIntegration dhcpL2RelayKafkaIntegration;
    private DhcpL2RelayListener dhcpL2RelayListener;

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
        Map.Entry<String, AtomicLong> entryCounter = Maps.immutableEntry(DHCP_COUNTER_TOPIC,
                new AtomicLong(1));
        DhcpAllocationInfo allocationInfo = new DhcpAllocationInfo(
                OLT_CONNECT_POINT,
                DHCP.MsgType.DHCPREQUEST, ONU_SERIAL, OLT_MAC,
                LOCAL_IP, SUBSCRIBER_ID);
        DhcpL2RelayEvent event = new DhcpL2RelayEvent(DhcpL2RelayEvent.Type.STATS_UPDATE,
                allocationInfo, OLT_CONNECT_POINT, entryCounter, null);
        dhcpL2RelayListener.event(event);
        assertEquals(MockEventBusService.dhcpStats, 1);
        assertEquals(MockEventBusService.otherCounter, 0);
    }

    /**
     * test to verify the DhcpL2RelayEvent.STATS_UPDATE event.
     */
    @Test
    void testDhcpL2RelayStatsSubscriberUpdate() {
        Map.Entry<String, AtomicLong> entryCounter = Maps.immutableEntry(DHCP_COUNTER_TOPIC, new AtomicLong(1));

        DhcpAllocationInfo allocationInfo = new DhcpAllocationInfo(
                OLT_CONNECT_POINT,
                DHCP.MsgType.DHCPREQUEST, ONU_SERIAL, OLT_MAC,
                LOCAL_IP, SUBSCRIBER_ID);
        DhcpL2RelayEvent event = new DhcpL2RelayEvent(DhcpL2RelayEvent.Type.STATS_UPDATE,
                allocationInfo, OLT_CONNECT_POINT, entryCounter, ONU_SERIAL);
        dhcpL2RelayListener.event(event);
        assertEquals(MockEventBusService.dhcpStats, 1);
        assertEquals(MockEventBusService.otherCounter, 0);
    }

    /**
     * test to verify the DhcpL2RelayEvent.UPDATE event.
     */
    @Test
    void testDhcpL2RelayUpdate() {
        ConnectPoint cp = new ConnectPoint(DEVICE_ID_1, PORT.number());
        Map.Entry<String, AtomicLong> entryCounter = Maps.immutableEntry(DHCP_COUNTER_TOPIC, new AtomicLong(1));
        DhcpAllocationInfo allocationInfo = new DhcpAllocationInfo(
                new ConnectPoint(DEVICE_ID_1, PORT.number()),
                DHCP.MsgType.DHCPREQUEST, ONU_SERIAL, OLT_MAC,
                LOCAL_IP, SUBSCRIBER_ID);
        DhcpL2RelayEvent event = new DhcpL2RelayEvent(DhcpL2RelayEvent.Type.UPDATED,
                allocationInfo, cp, entryCounter, null);
        dhcpL2RelayListener.event(event);
        assertEquals(MockEventBusService.dhcpEvents, 1);
        assertEquals(MockEventBusService.otherCounter, 0);
    }

    private static class MockEventBusService implements EventBusService {
        static int dhcpStats;
        static int dhcpEvents;
        static int otherCounter;

        MockEventBusService() {
            dhcpStats = 0;
            dhcpEvents = 0;
            otherCounter = 0;
        }

        @Override
        public void send(String topic, JsonNode data) {
            switch (topic) {
                case DhcpL2RelayKafkaIntegration.DHCP_STATS_TOPIC:
                    dhcpStats++;
                    break;
                case DhcpL2RelayKafkaIntegration.TOPIC:
                    dhcpEvents++;
                    break;
                default:
                    otherCounter++;
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
            dhcpL2RelayListener = null;
        }

        @Override
        public Map<String, DhcpAllocationInfo> getAllocationInfo() {
            return null;
        }

        @Override
        public void clearAllocations() {

        }

        @Override
        public boolean removeAllocationsByConnectPoint(ConnectPoint connectPoint) {
            return false;
        }
    }
}