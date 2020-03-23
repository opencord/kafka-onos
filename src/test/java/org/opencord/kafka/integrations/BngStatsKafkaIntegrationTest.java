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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.onlab.packet.IpAddress;
import org.onlab.packet.MacAddress;
import org.onlab.packet.VlanId;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.PortNumber;
import org.onosproject.net.behaviour.BngProgrammable;
import org.onosproject.net.pi.runtime.PiCounterCellData;
import org.opencord.bng.BngStatsEvent;
import org.opencord.bng.BngStatsEventListener;
import org.opencord.bng.BngStatsEventSubject;
import org.opencord.bng.BngStatsService;
import org.opencord.bng.PppoeBngAttachment;
import org.opencord.kafka.EventBusService;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * set of unit test cases to verify BngStatsKafkaIntegration.
 */
class BngStatsKafkaIntegrationTest {
    private static final VlanId C_TAG = VlanId.vlanId((short) 999);
    private static final VlanId S_TAG = VlanId.vlanId((short) 111);
    private static final String ONU_SERIAL = "TWSH00008084";

    private static final IpAddress LOCAL_IP = IpAddress.valueOf("127.0.0.1");
    private static final MacAddress OLT_MAC = MacAddress.valueOf("c6:b1:cd:40:dc:93");
    private static final PortNumber PORT_NUMBER = PortNumber.portNumber(1);
    private static final ConnectPoint OLT_CONNECT_POINT = new ConnectPoint(MockDeviceService.DEVICE_ID_1, PORT_NUMBER);
    private static final Short SESSION_ID = 2;
    private static final Short TPID = 8;

    private BngStatsKafkaIntegration bngStatsKafkaIntegration;
    private BngStatsEventListener bngStatsEventListener;

    @BeforeEach
    void setUp() {
        bngStatsKafkaIntegration = new BngStatsKafkaIntegration();
        bngStatsKafkaIntegration.eventBusService = new MockEventBusService();
        bngStatsKafkaIntegration.ignore = new MockBngStatsService();
        bngStatsKafkaIntegration.bindBngStatsService(bngStatsKafkaIntegration.ignore);
        bngStatsKafkaIntegration.activate();
    }

    @AfterEach
    void tearDown() {
        bngStatsKafkaIntegration.deactivate();
    }

    @Test
    void testBngStatsEvent() {
        PppoeBngAttachment pppoeBngAttachment = (PppoeBngAttachment) PppoeBngAttachment.builder()
                .withPppoeSessionId(SESSION_ID)
                .withCTag(C_TAG)
                .withIpAddress(LOCAL_IP)
                .withMacAddress(OLT_MAC)
                .withOltConnectPoint(OLT_CONNECT_POINT)
                .withOnuSerial(ONU_SERIAL)
                .withQinqTpid(TPID)
                .withSTag(S_TAG)
                .build();
        Map<BngProgrammable.BngCounterType, PiCounterCellData> attachmentStats = new HashMap<>();
        attachmentStats.put(BngProgrammable.BngCounterType.CONTROL_PLANE,
                new PiCounterCellData(1024, 1024));
        BngStatsEventSubject subject = new BngStatsEventSubject("PppoeKey",
                pppoeBngAttachment, attachmentStats);
        BngStatsEvent event = new BngStatsEvent(BngStatsEvent.EventType.STATS_UPDATED, subject);
        bngStatsEventListener.event(event);
        assertEquals(MockEventBusService.kafkaEvents, 1);
        assertEquals(MockEventBusService.otherCounter, 0);
    }

    private static class MockEventBusService implements EventBusService {
        static int kafkaEvents;
        static int otherCounter;

        @Override
        public void send(String topic, JsonNode data) {
            if (topic.equals("bng.stats")) {
                kafkaEvents++;
            } else {
                otherCounter++;
            }
        }
    }

    private class MockBngStatsService implements BngStatsService {
        @Override
        public Map<BngProgrammable.BngCounterType, PiCounterCellData> getStats(String s) {
            return null;
        }

        @Override
        public PiCounterCellData getControlStats() {
            return null;
        }

        @Override
        public void addListener(BngStatsEventListener listener) {
            bngStatsEventListener = listener;
        }

        @Override
        public void removeListener(BngStatsEventListener listener) {
            bngStatsEventListener = null;
        }
    }
}