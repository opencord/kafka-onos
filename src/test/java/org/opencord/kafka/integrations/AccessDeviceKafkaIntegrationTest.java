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
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.onlab.packet.ChassisId;
import org.onlab.packet.VlanId;
import org.onosproject.net.AnnotationKeys;
import org.onosproject.net.Annotations;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.DefaultAnnotations;
import org.onosproject.net.DefaultDevice;
import org.onosproject.net.Device;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Element;
import org.onosproject.net.ElementId;
import org.onosproject.net.Port;
import org.onosproject.net.PortNumber;
import org.onosproject.net.device.DeviceServiceAdapter;
import org.onosproject.net.driver.Behaviour;
import org.onosproject.net.provider.ProviderId;
import org.opencord.kafka.EventBusService;
import org.opencord.olt.AccessDeviceEvent;
import org.opencord.olt.AccessDeviceListener;
import org.opencord.olt.AccessDeviceService;
import org.opencord.olt.AccessSubscriberId;
import org.opencord.sadis.UniTagInformation;

import java.util.List;
import java.util.Optional;
import java.util.Set;

class AccessDeviceKafkaIntegrationTest {

    private static final VlanId CLIENT_C_TAG = VlanId.vlanId((short) 999);
    private static final VlanId CLIENT_S_TAG = VlanId.vlanId((short) 111);
    private static final String OLT_DEV_ID = "of:0000c6b1cd40dc93";
    private static final DeviceId DEVICE_ID_1 = DeviceId.deviceId(OLT_DEV_ID);
    private static final String SCHEME_NAME = "accessDevice";
    private static final DefaultAnnotations DEVICE_ANNOTATIONS = DefaultAnnotations.builder()
            .set(AnnotationKeys.PROTOCOL, SCHEME_NAME.toUpperCase()).build();

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

    @Test
    void testUniAdded() {
        Port p = new MockPort();
        AccessDeviceEvent event = new AccessDeviceEvent(AccessDeviceEvent.Type.UNI_ADDED,
                DEVICE_ID_1, p, CLIENT_S_TAG, CLIENT_C_TAG, 8);
        accessDeviceListener.event(event);
        Assert.assertNotEquals(MockEventBusService.kafkaEvents, 0);
    }

    @Test
    void testUniRemoved() {
        Port p = new MockPort();

        AccessDeviceEvent event = new AccessDeviceEvent(AccessDeviceEvent.Type.UNI_REMOVED,
                DEVICE_ID_1, p, CLIENT_S_TAG, CLIENT_C_TAG, 8);
        accessDeviceListener.event(event);
        Assert.assertNotEquals(MockEventBusService.kafkaEvents, 0);
    }

    private static class MockEventBusService implements EventBusService {
        static int kafkaEvents = 0;

        @Override
        public void send(String topic, JsonNode data) {
            if (topic.equals("onu.events")) {
                kafkaEvents++;
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

    private static class MockPort implements Port {

        @Override
        public boolean isEnabled() {
            return true;
        }

        public long portSpeed() {
            return 1000;
        }

        public Element element() {
            return new Element() {
                @Override
                public ElementId id() {
                    return DEVICE_ID_1;
                }

                @Override
                public Annotations annotations() {
                    return null;
                }

                @Override
                public ProviderId providerId() {
                    return null;
                }

                @Override
                public <B extends Behaviour> B as(Class<B> projectionClass) {
                    return null;
                }

                @Override
                public <B extends Behaviour> boolean is(Class<B> projectionClass) {
                    return false;
                }
            };
        }

        public PortNumber number() {
            return PortNumber.portNumber(1);
        }

        public Annotations annotations() {
            return new AccessDeviceKafkaIntegrationTest.MockPort.MockAnnotations();
        }

        public Type type() {
            return Port.Type.FIBER;
        }

        private static class MockAnnotations implements Annotations {

            @Override
            public String value(String val) {
                return "UNI-1";
            }

            public Set<String> keys() {
                return null;
            }
        }
    }

    private static class MockDeviceService extends DeviceServiceAdapter {

        private final ProviderId providerId = new ProviderId("of", "foo");
        private final Device device1 = new AccessDeviceKafkaIntegrationTest.MockDevice(
                providerId, DEVICE_ID_1, Device.Type.SWITCH,
                "foo.inc", "0", "0",
                OLT_DEV_ID, new ChassisId(),
                DEVICE_ANNOTATIONS);

        @Override
        public Device getDevice(DeviceId devId) {
            return device1;
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
        public boolean provisionSubscriber(AccessSubscriberId accessSubscriberId,
                                           Optional<VlanId> optional, Optional<VlanId> optional1,
                                           Optional<Integer> optional2) {
            return false;
        }

        @Override
        public boolean removeSubscriber(AccessSubscriberId accessSubscriberId,
                                        Optional<VlanId> optional, Optional<VlanId> optional1,
                                        Optional<Integer> optional2) {
            return false;
        }

        @Override
        public List<DeviceId> fetchOlts() {
            return null;
        }

        @Override
        public ImmutableMap<ConnectPoint, Set<UniTagInformation>> getProgSubs() {
            return null;
        }

        @Override
        public void addListener(AccessDeviceListener listener) {
            accessDeviceListener = listener;
        }

        @Override
        public void removeListener(AccessDeviceListener listener) {

        }
    }
}