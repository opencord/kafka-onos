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
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.onlab.packet.ChassisId;
import org.onosproject.net.AnnotationKeys;
import org.onosproject.net.Annotations;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.DefaultAnnotations;
import org.onosproject.net.DefaultDevice;
import org.onosproject.net.Device;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Element;
import org.onosproject.net.Port;
import org.onosproject.net.PortNumber;
import org.onosproject.net.device.DeviceEvent;
import org.onosproject.net.device.DeviceListener;
import org.onosproject.net.device.DeviceServiceAdapter;
import org.onosproject.net.device.PortStatistics;
import org.onosproject.net.provider.ProviderId;
import org.opencord.kafka.EventBusService;

import java.util.List;
import java.util.Set;

class DeviceKafkaIntegrationTest {
    private static final String OLT_DEV_ID = "of:0000c6b1cd40dc93";
    private static final DeviceId DEVICE_ID_1 = DeviceId.deviceId(OLT_DEV_ID);
    private static final String SCHEME_NAME = "deviceKafka";
    private static final DefaultAnnotations DEVICE_ANNOTATIONS = DefaultAnnotations.builder()
            .set(AnnotationKeys.PROTOCOL, SCHEME_NAME.toUpperCase()).build();

    private DeviceKafkaIntegration deviceKafkaIntegration;
    private DeviceListener deviceListener;

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

    @Test
    void testPortStateUpdate() {
        DeviceEvent event = new DeviceEvent(DeviceEvent.Type.PORT_UPDATED,
                deviceKafkaIntegration.deviceService.getDevice(DEVICE_ID_1),
                new MockPort());
        deviceListener.event(event);
        Assert.assertNotEquals(MockEventBusService.events, 0);
    }

    @Test
    void testPortStatsUpdate() {
        DeviceEvent event = new DeviceEvent(DeviceEvent.Type.PORT_STATS_UPDATED,
                deviceKafkaIntegration.deviceService.getDevice(DEVICE_ID_1),
                new MockPort());
        deviceListener.event(event);
        Assert.assertNotEquals(MockEventBusService.kpis, 0);
    }


    private static class MockPort implements Port {

        @Override
        public boolean isEnabled() {
            return true;
        }

        public long portSpeed() {
            return 1000;
        }

        public Element element() {
            return null;
        }

        public PortNumber number() {
            return PortNumber.portNumber(1);
        }

        public Annotations annotations() {
            return new DeviceKafkaIntegrationTest
                    .MockPort.MockAnnotations();
        }

        public Type type() {
            return Port.Type.FIBER;
        }

        private static class MockAnnotations implements Annotations {

            @Override
            public String value(String val) {
                return "nni-";
            }

            public Set<String> keys() {
                return Sets.newHashSet("portName");
            }
        }
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

            if (topic.equals("onos.kpis")) {
                kpis++;
            }
            if (topic.equals("onos.events.port")) {
                events++;
            }
        }
    }

    /*
        Mocks Device Service Adapter.
         */
    private class MockDeviceService extends DeviceServiceAdapter {

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
        public Port getPort(ConnectPoint cp) {
            return new DeviceKafkaIntegrationTest.MockPort();
        }

        @Override
        public Port getPort(DeviceId deviceId, PortNumber portNumber) {
            return new DeviceKafkaIntegrationTest.MockPort();
        }

        @Override
        public List<Port> getPorts(DeviceId deviceId) {
            return Lists.newArrayList(new DeviceKafkaIntegrationTest.MockPort());
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
        public boolean isAvailable(DeviceId d) {
            return true;
        }

        @Override
        public void addListener(DeviceListener listener) {
            deviceListener = listener;
        }

        @Override
        public void removeListener(DeviceListener listener) {

        }
    }
}