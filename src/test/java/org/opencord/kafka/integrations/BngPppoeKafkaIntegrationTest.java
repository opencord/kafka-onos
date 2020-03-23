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
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.onlab.packet.IpAddress;
import org.onlab.packet.MacAddress;
import org.onlab.packet.VlanId;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.PortNumber;
import org.opencord.bng.PppoeBngControlHandler;
import org.opencord.bng.PppoeEvent;
import org.opencord.bng.PppoeEventListener;
import org.opencord.bng.PppoeEventSubject;
import org.opencord.kafka.EventBusService;

import static org.opencord.kafka.integrations.MockDeviceService.DEVICE_ID_1;

/**
 * set of unit test cases for BngPppoeKafkaIntegration.
 */
class BngPppoeKafkaIntegrationTest {
    private static final VlanId C_TAG = VlanId.vlanId((short) 999);
    private static final VlanId S_TAG = VlanId.vlanId((short) 111);
    private static final String TOPIC_PPPOE = "bng.pppoe";
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
        PppoeEvent event = new PppoeEvent(PppoeEvent.EventType.AUTH_SUCCESS,
                new PppoeEventSubject(new ConnectPoint(DEVICE_ID_1, PortNumber.portNumber(1)),
                        IpAddress.valueOf("127.0.0.1"), MacAddress.valueOf("c6:b1:cd:40:dc:93"),
                        "TWSH00008084", (short) 2, S_TAG, C_TAG));
        pppoeEventListener.event(event);
        Assert.assertNotEquals(MockEventBusService.kafkaEvents, 0);

    }

    private static class MockEventBusService implements EventBusService {
        static int kafkaEvents = 0;

        @Override
        public void send(String topic, JsonNode data) {
            if (topic.equals(TOPIC_PPPOE)) {
                kafkaEvents++;
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