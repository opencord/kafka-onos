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
import org.onlab.packet.VlanId;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.DeviceId;
import org.opencord.kafka.EventBusService;
import org.opencord.olt.AccessDeviceListener;
import org.opencord.olt.AccessDeviceService;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Set of unit test cases for AccessDeviceKafkaIntegration.
 */
class AccessDeviceKafkaIntegrationTest extends KafkaIntegrationTestBase {

    private AccessDeviceKafkaIntegration accessDeviceKakfa;
    private AccessDeviceListener accessDeviceListener;

    @BeforeEach
    void setUp() {
        accessDeviceKakfa = new AccessDeviceKafkaIntegration();

        accessDeviceKakfa.deviceService = new MockDeviceService();
        accessDeviceKakfa.eventBusService = new MockEventBusService();
        accessDeviceKakfa.accessDeviceService = new MockAccessDeviceService();
        accessDeviceKakfa.bindAccessDeviceService(accessDeviceKakfa.accessDeviceService);
        accessDeviceKakfa.activate();
    }

    @AfterEach
    void tearDown() {
        accessDeviceKakfa.deactivate();
        accessDeviceKakfa = null;
    }

    /**
     * testcase to perform UNI_ADDED AccessDeviceEvent.
     */
    @Test
    void testUniAdded() {
        accessDeviceListener.event(getUniAdded());
        assertEquals(MockEventBusService.kafkaEvents, 1);
        assertEquals(MockEventBusService.otherCounter, 0);
    }

    /**
     * testcase to perform UNI_REMOVED AccessDeviceEvent.
     */
    @Test
    void testUniRemoved() {
        accessDeviceListener.event(getUniRemoved());
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
            if (topic.equals(AccessDeviceKafkaIntegration.TOPIC)) {
                kafkaEvents++;
            } else {
                otherCounter++;
            }
        }
    }

    private class MockAccessDeviceService implements AccessDeviceService {
        @Override
        public boolean provisionSubscriber(ConnectPoint connectPoint) {
            return false;
        }

        @Override
        public boolean removeSubscriber(ConnectPoint connectPoint) {
            return false;
        }

        @Override
        public boolean provisionSubscriber(ConnectPoint accessSubscriberId,
                                           VlanId optional, VlanId optional1,
                                           Integer optional2) {
            return false;
        }

        @Override
        public boolean removeSubscriber(ConnectPoint accessSubscriberId,
                                        VlanId optional, VlanId optional1,
                                        Integer optional2) {
            return false;
        }

        @Override
        public List<DeviceId> getConnectedOlts() {
            return null;
        }

        @Override
        public ConnectPoint findSubscriberConnectPoint(String id) {
            return null;
        }

        @Override
        public void addListener(AccessDeviceListener listener) {
            accessDeviceListener = listener;
        }

        @Override
        public void removeListener(AccessDeviceListener listener) {
            accessDeviceListener = null;
        }
    }
}