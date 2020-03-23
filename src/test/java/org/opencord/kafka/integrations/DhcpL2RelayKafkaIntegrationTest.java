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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.onlab.packet.ChassisId;
import org.onlab.packet.DHCP;
import org.onlab.packet.IpAddress;
import org.onlab.packet.MacAddress;
import org.onosproject.cluster.ClusterServiceAdapter;
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
import org.onosproject.net.device.DeviceListener;
import org.onosproject.net.device.DeviceServiceAdapter;
import org.onosproject.net.provider.ProviderId;
import org.opencord.dhcpl2relay.DhcpAllocationInfo;
import org.opencord.dhcpl2relay.DhcpL2RelayEvent;
import org.opencord.dhcpl2relay.DhcpL2RelayListener;
import org.opencord.dhcpl2relay.DhcpL2RelayService;
import org.opencord.kafka.EventBusService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

class DhcpL2RelayKafkaIntegrationTest {

    private static final String OLT_DEV_ID = "of:0000c6b1cd40dc93";
    private static final DeviceId DEVICE_ID_1 = DeviceId.deviceId(OLT_DEV_ID);
    private static final String SCHEME_NAME = "dhcpl2relay";
    private static final MacAddress OLT_MAC_ADDRESS = MacAddress.valueOf("01:02:03:04:05:06");
    private static final DefaultAnnotations DEVICE_ANNOTATIONS = DefaultAnnotations.builder()
            .set(AnnotationKeys.PROTOCOL, SCHEME_NAME.toUpperCase()).build();

    private DhcpL2RelayKafkaIntegration dhcpL2RelayKafkaIntegration;
    private DhcpL2RelayListener dhcpL2RelayListener;

    @BeforeEach
    void setUp() {
        dhcpL2RelayKafkaIntegration = new DhcpL2RelayKafkaIntegration();
        dhcpL2RelayKafkaIntegration.clusterService = new ClusterServiceAdapter();
        dhcpL2RelayKafkaIntegration.deviceService = new MockDeviceService();
        dhcpL2RelayKafkaIntegration.eventBusService = new MockEventBusService();
        dhcpL2RelayKafkaIntegration.ignore = new MockDhcpL2RelayService();
        dhcpL2RelayKafkaIntegration.bindDhcpL2RelayService(dhcpL2RelayKafkaIntegration.ignore);
        dhcpL2RelayKafkaIntegration.activate();
    }

    @AfterEach
    void tearDown() {
        dhcpL2RelayKafkaIntegration.deactivate();
    }

    @Test
    void testDhcpL2RelayStatsUpdate() {
        Port port = new MockPort();
        ConnectPoint cp = new ConnectPoint(DEVICE_ID_1, port.number());
        Map.Entry<String, AtomicLong> entryCounter = Maps.immutableEntry("DHCPREQUEST", new AtomicLong(1));
        String dhcpCountersTopic = "DHCPREQUEST";
        String subscriber = null;
        DhcpAllocationInfo allocationInfo = new DhcpAllocationInfo(
                new ConnectPoint(DEVICE_ID_1, port.number()),
                DHCP.MsgType.DHCPREQUEST, "TWSH00008084", OLT_MAC_ADDRESS,
                IpAddress.valueOf("127.0.0.1"));
        DhcpL2RelayEvent event = new DhcpL2RelayEvent(DhcpL2RelayEvent.Type.STATS_UPDATE,
                allocationInfo, cp, entryCounter, dhcpCountersTopic, subscriber);
        dhcpL2RelayListener.event(event);
        Assert.assertNotEquals(MockEventBusService.dhcpStats, 0);
    }

    @Test
    void testDhcpL2RelayStatsSubscriberUpdate() {
        Port port = new MockPort();
        ConnectPoint cp = new ConnectPoint(DEVICE_ID_1, port.number());
        Map.Entry<String, AtomicLong> entryCounter = Maps.immutableEntry("DHCPREQUEST", new AtomicLong(1));
        String dhcpCountersTopic = "DHCPREQUEST";
        String subscriber = "TWSH00008084";
        DhcpAllocationInfo allocationInfo = new DhcpAllocationInfo(
                new ConnectPoint(DEVICE_ID_1, port.number()),
                DHCP.MsgType.DHCPREQUEST, "TWSH00008084", OLT_MAC_ADDRESS,
                IpAddress.valueOf("127.0.0.1"));
        DhcpL2RelayEvent event = new DhcpL2RelayEvent(DhcpL2RelayEvent.Type.STATS_UPDATE,
                allocationInfo, cp, entryCounter, dhcpCountersTopic, subscriber);
        dhcpL2RelayListener.event(event);
        Assert.assertNotEquals(MockEventBusService.dhcpStats, 0);
    }
    @Test
    void testDhcpL2RelayUpdate() {
        Port port = new MockPort();
        ConnectPoint cp = new ConnectPoint(DEVICE_ID_1, port.number());
        Map.Entry<String, AtomicLong> entryCounter = Maps.immutableEntry("DHCPREQUEST", new AtomicLong(1));
        String dhcpCountersTopic = "DHCPREQUEST";
        String subscriber = null;
        DhcpAllocationInfo allocationInfo = new DhcpAllocationInfo(
                new ConnectPoint(DEVICE_ID_1, port.number()),
                DHCP.MsgType.DHCPREQUEST, "TWSH00008084", OLT_MAC_ADDRESS,
                IpAddress.valueOf("127.0.0.1"));
        DhcpL2RelayEvent event = new DhcpL2RelayEvent(DhcpL2RelayEvent.Type.UPDATED,
                allocationInfo, cp, entryCounter, dhcpCountersTopic, subscriber);
        dhcpL2RelayListener.event(event);
        Assert.assertNotEquals(MockEventBusService.dhcpEvents, 0);
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
            return new DhcpL2RelayKafkaIntegrationTest.MockPort.MockAnnotations();
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
        static int dhcpStats = 0;
        static int dhcpEvents = 0;

        @Override
        public void send(String topic, JsonNode data) {
            switch (topic) {
                case "onos.dhcp.stats.kpis":
                    dhcpStats++;
                    break;
                case "dhcp.events":
                    dhcpEvents++;
                    break;
                default:
                    break;
            }
        }
    }

    /*
    Mocks Device Service Adapter.
     */
    private static class MockDeviceService extends DeviceServiceAdapter {

        private final ProviderId providerId = new ProviderId("of", "foo");
        private final Device device1 = new DhcpL2RelayKafkaIntegrationTest
                .MockDevice(providerId,
                DEVICE_ID_1, Device.Type.SWITCH,
                "foo.inc", "0", "0",
                OLT_DEV_ID, new ChassisId(), DEVICE_ANNOTATIONS);

        @Override
        public Device getDevice(DeviceId devId) {
            return device1;

        }

        @Override
        public Iterable<Device> getDevices() {
            List<Device> devices = new ArrayList<>();
            devices.add(device1);
            return devices;
        }

        @Override
        public Port getPort(ConnectPoint cp) {
            return new DhcpL2RelayKafkaIntegrationTest.MockPort();
        }

        @Override
        public Port getPort(DeviceId deviceId, PortNumber portNumber) {
            return new DhcpL2RelayKafkaIntegrationTest.MockPort();
        }

        @Override
        public List<Port> getPorts(DeviceId deviceId) {
            return Lists.newArrayList(new DhcpL2RelayKafkaIntegrationTest.MockPort());
        }

        @Override
        public boolean isAvailable(DeviceId d) {
            return true;
        }

        @Override
        public void addListener(DeviceListener listener) {
//            deviceListener = listener;
        }

        @Override
        public void removeListener(DeviceListener listener) {

        }
    }

    private class MockDhcpL2RelayService implements DhcpL2RelayService {
        @Override
        public void addListener(DhcpL2RelayListener listener) {
            dhcpL2RelayListener = listener;
        }

        @Override
        public void removeListener(DhcpL2RelayListener listener) {

        }
    }
}