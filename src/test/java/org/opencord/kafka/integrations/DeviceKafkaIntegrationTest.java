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
import org.onosproject.net.device.DeviceEvent;
import org.opencord.kafka.EventBusService;

import static org.junit.Assert.assertEquals;
import static org.opencord.kafka.integrations.MockDeviceService.DEVICE_ID_1;

/**
 * set of unit test cases for DeviceKafkaIntegration.
 */
class DeviceKafkaIntegrationTest extends KafkaIntegrationTestBase {
    private DeviceKafkaIntegration deviceKafkaIntegration;

    @BeforeEach
    void setUp() {
        deviceKafkaIntegration = new DeviceKafkaIntegration();
        deviceKafkaIntegration.eventBusService = new MockEventBusService();
        deviceKafkaIntegration.deviceService = new MockDeviceService();
        deviceKafkaIntegration.activate();
    }

    @AfterEach
    void tearDown() {
        deviceKafkaIntegration.deactivate();
    }

    /**
     * testcase to verify Port updated event.
     */
    @Test
    void testPortStateUpdate() {
        DeviceEvent event = new DeviceEvent(DeviceEvent.Type.PORT_UPDATED,
                deviceKafkaIntegration.deviceService.getDevice(DEVICE_ID_1),
                new MockDeviceService.MockPort());
        deviceKafkaIntegration.listener.event(event);
        assertEquals(MockEventBusService.events, 1);
        assertEquals(MockEventBusService.otherCounter, 0);
    }

    /**
     * testcase to verify Port stats update event.
     */
    @Test
    void testPortStatsUpdate() {
        DeviceEvent event = new DeviceEvent(DeviceEvent.Type.PORT_STATS_UPDATED,
                deviceKafkaIntegration.deviceService.getDevice(DEVICE_ID_1),
                new MockDeviceService.MockPort());
        deviceKafkaIntegration.listener.event(event);
        assertEquals(MockEventBusService.kpis, 1);
        assertEquals(MockEventBusService.otherCounter, 0);
    }

    private static class MockEventBusService implements EventBusService {
        static int kpis;
        static int events;
        static int otherCounter;

        MockEventBusService() {
            kpis = 0;
            events = 0;
            otherCounter = 0;
        }

        @Override
        public void send(String topic, JsonNode data) {
            switch (topic) {
                case DeviceKafkaIntegration.TOPIC:
                    kpis++;
                    break;
                case DeviceKafkaIntegration.PORT_EVENT_TOPIC:
                    events++;
                    break;
                default:
                    otherCounter++;
                    break;
            }
        }
    }
}