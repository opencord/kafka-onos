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
import org.onosproject.net.behaviour.BngProgrammable;
import org.onosproject.net.pi.runtime.PiCounterCellData;
import org.opencord.bng.BngStatsEventListener;
import org.opencord.bng.BngStatsService;
import org.opencord.kafka.EventBusService;

import java.util.Map;

class BngStatsKafkaIntegrationTest {
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

//    @Test
//    void testBngStatsEvent() {
//        BngAttachment bngAttachment = PppoeBngAttachment.builder().build();
//        BngStatsEventSubject subject = new BngStatsEventSubject("PppoeKey",
//                bngAttachment, Maps.newHashMap());
//        BngStatsEvent event = new BngStatsEvent(BngStatsEvent.EventType.STATS_UPDATED, subject);
//        bngStatsEventListener.event(event);
//    }

    private static class MockEventBusService implements EventBusService {
        static int kafkaEvents = 0;

        @Override
        public void send(String topic, JsonNode data) {
            if (topic.equals("bng.stats")) {
                kafkaEvents++;
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

        }
    }
//    private class MockBngAttachment implements BngProgrammable.Attachment {
//        @Override
//        public ApplicationId appId() {
//            return ApplicationId;
//        }
//
//        @Override
//        public VlanId sTag() {
//            return null;
//        }
//
//        @Override
//        public VlanId cTag() {
//            return null;
//        }
//
//        @Override
//        public MacAddress macAddress() {
//            return null;
//        }
//
//        @Override
//        public IpAddress ipAddress() {
//            return null;
//        }
//
//        @Override
//        public boolean lineActive() {
//            return false;
//        }
//
//        @Override
//        public AttachmentType type() {
//            return null;
//        }
//
//        @Override
//        public short pppoeSessionId() {
//            return 0;
//        }
//        public MockBngAttachment(ApplicationId appId, VlanId sTag, VlanId cTag, MacAddress macAddress,
//        IpAddress ipAddress, boolean lineActivated, ConnectPoint oltConnectPoint, String onuSerial, short qinqTpid) {
//            super( appId,  sTag,  cTag,  macAddress,  ipAddress,
//            lineActivated,  oltConnectPoint,  onuSerial,  qinqTpid);
//
//        }
//
//
//    }
}