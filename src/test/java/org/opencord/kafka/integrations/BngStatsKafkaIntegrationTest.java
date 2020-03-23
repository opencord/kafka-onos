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
import org.onosproject.net.behaviour.BngProgrammable;
import org.onosproject.net.pi.runtime.PiCounterCellData;
import org.opencord.bng.BngStatsEventListener;
import org.opencord.bng.BngStatsService;
import org.opencord.kafka.EventBusService;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * set of unit test cases to verify BngStatsKafkaIntegration.
 */
class BngStatsKafkaIntegrationTest extends KafkaIntegrationTestBase {


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
        bngStatsEventListener.event(getBngStatsEvent());
        assertEquals(MockEventBusService.kafkaEvents, 1);
        assertEquals(MockEventBusService.otherCounter, 0);
    }

    private static class MockEventBusService implements EventBusService {
        static int kafkaEvents;
        static int otherCounter;

        MockEventBusService() {
            kafkaEvents = 0;
            otherCounter = 0;
        }

        @Override
        public void send(String topic, JsonNode data) {
            if (topic.equals(BngStatsKafkaIntegration.TOPIC_STATS)) {
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