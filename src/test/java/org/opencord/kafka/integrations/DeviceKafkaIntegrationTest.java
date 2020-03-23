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
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.onlab.packet.ChassisId;
import org.onosproject.net.AnnotationKeys;
import org.onosproject.net.Annotations;
import org.onosproject.net.DefaultAnnotations;
import org.onosproject.net.DefaultDevice;
import org.onosproject.net.Device;
import org.onosproject.net.DeviceId;
import org.onosproject.net.PortNumber;
import org.onosproject.net.device.DeviceEvent;
import org.onosproject.net.device.DeviceListener;
import org.onosproject.net.device.DeviceServiceAdapter;
import org.onosproject.net.device.PortStatistics;
import org.onosproject.net.provider.ProviderId;
import org.opencord.kafka.EventBusService;

import java.util.List;

import static org.opencord.kafka.integrations.MockDeviceService.DEVICE_ID_1;
import static org.opencord.kafka.integrations.MockDeviceService.OLT_DEV_ID;

/**
 * set of unit test cases for DeviceKafkaIntegration.
 */
class DeviceKafkaIntegrationTest {
    private static final String SCHEME_NAME = "deviceKafka";
    private static final DefaultAnnotations DEVICE_ANNOTATIONS = DefaultAnnotations.builder()
            .set(AnnotationKeys.PROTOCOL, SCHEME_NAME.toUpperCase()).build();
    private DeviceKafkaIntegration deviceKafkaIntegration;
    private DeviceListener deviceListener;

    @BeforeEach
    void setUp() {
        deviceKafkaIntegration = new DeviceKafkaIntegration();
        deviceKafkaIntegration.eventBusService = new MockEventBusService();
        deviceKafkaIntegration.deviceService = new MockerDeviceService();
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
        deviceListener.event(event);
        Assert.assertNotEquals(MockEventBusService.events, 0);
    }

    /**
     * testcase to verify Port stats update event.
     */
    @Test
    void testPortStatsUpdate() {
        DeviceEvent event = new DeviceEvent(DeviceEvent.Type.PORT_STATS_UPDATED,
                deviceKafkaIntegration.deviceService.getDevice(DEVICE_ID_1),
                new MockDeviceService.MockPort());
        deviceListener.event(event);
        Assert.assertNotEquals(MockEventBusService.kpis, 0);
    }

    private static class MockDevice extends DefaultDevice {

        /*
        Mocks OLT device.
         */
        public MockDevice(ProviderId providerId, DeviceId id, Type type,
                          String manufacturer, String hwVersion, String swVersion,
                          String serialNumber, ChassisId chassisId, Annotations... annotations) {
            super(providerId, id, type, manufacturer, hwVersion, swVersion, serialNumber,
                    chassisId, annotations);
        }
    }

    private static class MockEventBusService implements EventBusService {
        static int kpis = 0;
        static int events = 0;

        @Override
        public void send(String topic, JsonNode data) {

            if (topic.equals(DeviceKafkaIntegration.TOPIC)) {
                kpis++;
            }
            if (topic.equals(DeviceKafkaIntegration.PORT_EVENT_TOPIC)) {
                events++;
            }
        }
    }

    /*
       Mocks Device Service Adapter.
    */
    private class MockerDeviceService extends DeviceServiceAdapter {

        private final ProviderId providerId = new ProviderId("of", "foo");
        private final Device device1 = new DeviceKafkaIntegrationTest.MockDevice(
                providerId, DEVICE_ID_1, Device.Type.SWITCH,
                "foo.inc", "0", "0", OLT_DEV_ID, new ChassisId(),
                DEVICE_ANNOTATIONS);

        @Override
        public Device getDevice(DeviceId devId) {
            return device1;
        }

        @Override
        public List<PortStatistics> getPortStatistics(DeviceId deviceId) {
            PortStatistics ps = new PortStatistics() {
                @Override
                public PortNumber portNumber() {
                    return PortNumber.portNumber(1);
                }

                @Override
                public long packetsReceived() {
                    return 100;
                }

                @Override
                public long packetsSent() {
                    return 10;
                }

                @Override
                public long bytesReceived() {
                    return 100;
                }

                @Override
                public long bytesSent() {
                    return 10;
                }

                @Override
                public long packetsRxDropped() {
                    return 0;
                }

                @Override
                public long packetsTxDropped() {
                    return 1;
                }

                @Override
                public long packetsRxErrors() {
                    return 1;
                }

                @Override
                public long packetsTxErrors() {
                    return 1;
                }

                @Override
                public long durationSec() {
                    return 100;
                }

                @Override
                public long durationNano() {
                    return 100 * 1000;
                }

                @Override
                public boolean isZero() {
                    return false;
                }
            };
            return Lists.newArrayList(ps);
        }

        @Override
        public void addListener(DeviceListener listener) {
            deviceListener = listener;
        }

        @Override
        public void removeListener(DeviceListener listener) {
            deviceListener = null;
        }
    }
}