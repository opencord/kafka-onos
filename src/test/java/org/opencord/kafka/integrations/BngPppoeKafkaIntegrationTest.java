/*
 * Copyright 2018-2023 Open Networking Foundation (ONF) and the ONF Contributors
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
import org.opencord.bng.PppoeBngControlHandler;
import org.opencord.bng.PppoeEventListener;
import org.opencord.kafka.EventBusService;

import static org.junit.Assert.assertEquals;

/**
 * set of unit test cases for BngPppoeKafkaIntegration.
 */
class BngPppoeKafkaIntegrationTest extends KafkaIntegrationTestBase {

    private BngPppoeKafkaIntegration bngPppoeKafka;
    private PppoeEventListener pppoeEventListener;

    @BeforeEach
    void setUp() {
        bngPppoeKafka = new BngPppoeKafkaIntegration();
        bngPppoeKafka.eventBusService = new MockEventBusService();
        bngPppoeKafka.ignore = new MockPppoeBngControlHandler();
        bngPppoeKafka.bindPppoeBngControl(bngPppoeKafka.ignore);
        bngPppoeKafka.activate();
    }

    @AfterEach
    void tearDown() {
        bngPppoeKafka.deactivate();
    }

    /**
     * testcase to perform PppoeEvent.
     */
    @Test
    void testbngPpoeEvent() {
        pppoeEventListener.event(getPppoeEvent());
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
            if (topic.equals(BngPppoeKafkaIntegration.TOPIC_PPPOE)) {
                kafkaEvents++;
            } else {
                otherCounter++;
            }
        }
    }

    private class MockPppoeBngControlHandler implements PppoeBngControlHandler {
        @Override
        public void addListener(PppoeEventListener listener) {
            pppoeEventListener = listener;
        }

        @Override
        public void removeListener(PppoeEventListener listener) {
            pppoeEventListener = null;
        }
    }
}