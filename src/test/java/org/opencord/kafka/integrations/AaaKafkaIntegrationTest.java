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
import org.onlab.packet.MacAddress;
import org.onlab.packet.RADIUS;
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
import org.opencord.aaa.AaaMachineStatisticsDelegate;
import org.opencord.aaa.AaaMachineStatisticsEvent;
import org.opencord.aaa.AaaMachineStatisticsEventListener;
import org.opencord.aaa.AaaMachineStatisticsService;
import org.opencord.aaa.AaaStatistics;
import org.opencord.aaa.AaaStatisticsSnapshot;
import org.opencord.aaa.AaaSupplicantMachineStats;
import org.opencord.aaa.AuthenticationEvent;
import org.opencord.aaa.AuthenticationEventListener;
import org.opencord.aaa.AuthenticationRecord;
import org.opencord.aaa.AuthenticationService;
import org.opencord.aaa.AuthenticationStatisticsEvent;
import org.opencord.aaa.AuthenticationStatisticsEventListener;
import org.opencord.aaa.AuthenticationStatisticsService;
import org.opencord.aaa.RadiusCommunicator;
import org.opencord.aaa.RadiusOperationalStatusEvent;
import org.opencord.aaa.RadiusOperationalStatusEventDelegate;
import org.opencord.aaa.RadiusOperationalStatusEventListener;
import org.opencord.aaa.RadiusOperationalStatusService;
import org.opencord.kafka.EventBusService;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.slf4j.LoggerFactory.getLogger;

class AaaKafkaIntegrationTest {
    private static final Logger log = getLogger(AaaKafkaIntegrationTest.class);

    private static final String OLT_DEV_ID = "of:0000c6b1cd40dc93";
    private static final DeviceId DEVICE_ID_1 = DeviceId.deviceId(OLT_DEV_ID);
    private static final String SCHEME_NAME = "aaaKafka";
    private static final DefaultAnnotations DEVICE_ANNOTATIONS = DefaultAnnotations.builder()
            .set(AnnotationKeys.PROTOCOL, SCHEME_NAME.toUpperCase()).build();

    private AaaKafkaIntegration aaaKafkaInt;
    private DeviceListener deviceListener;
    private AuthenticationEventListener authEventListener;
    private AuthenticationStatisticsEventListener authStatisticsEventListener;
    private RadiusOperationalStatusEventListener radiusOperStatusEventListener;
    private AaaMachineStatisticsEventListener aaaMachineStatisticsEventListener;


    @BeforeEach
    void setUp() {
        aaaKafkaInt = new AaaKafkaIntegration();

        aaaKafkaInt.deviceService = new MockDeviceService();
        aaaKafkaInt.eventBusService = new MockEventBusService();
        aaaKafkaInt.ignore = new MockAuthenticationService();
        aaaKafkaInt.ignore2 = new MockAuthenticationStatisticsService();
        aaaKafkaInt.ignore3 = new MockRadiusOperationalStatusService();
        aaaKafkaInt.ignore4 = new MockAaaMachineStatisticsService();
        aaaKafkaInt.bindAuthenticationService(aaaKafkaInt.ignore);
        aaaKafkaInt.bindAuthenticationStatService(aaaKafkaInt.ignore2);
        aaaKafkaInt.bindRadiusOperationalStatusService(aaaKafkaInt.ignore3);
        aaaKafkaInt.bindAaaMachineStatisticsService(aaaKafkaInt.ignore4);
        aaaKafkaInt.activate();
    }

    @AfterEach
    void tearDown() {
        aaaKafkaInt.deactivate();
        aaaKafkaInt.unbindRadiusOperationalStatusService(aaaKafkaInt.ignore3);
        aaaKafkaInt.unbindAaaMachineStatisticsService(aaaKafkaInt.ignore4);
        aaaKafkaInt = null;
    }

    @Test
    void testAuthenticationEvent() {
        AuthenticationEvent event = new AuthenticationEvent(AuthenticationEvent.Type.APPROVED,
                new ConnectPoint(DEVICE_ID_1, PortNumber.portNumber(1)));
        authEventListener.event(event);
        Assert.assertNotEquals(MockEventBusService.authCounter, 0);
        log.info("testAuthenticationEvent: executed successfully");
    }

    @Test
    void testAuthenticationStatisticsEvent() {
        AuthenticationStatisticsEvent event = new AuthenticationStatisticsEvent(
                AuthenticationStatisticsEvent.Type.STATS_UPDATE,
                new AaaStatistics());
        authStatisticsEventListener.event(event);
        Assert.assertNotEquals(MockEventBusService.authStatsCounter, 0);
        log.info("testAuthenticationStatisticsEvent: executed successfully");
    }

    @Test
    void testRadiusOperationalStatusEvent() {
        RadiusOperationalStatusEvent event = new RadiusOperationalStatusEvent(
                RadiusOperationalStatusEvent.Type.RADIUS_OPERATIONAL_STATUS,
                "AUTHENTICATED");
        radiusOperStatusEventListener.event(event);
        Assert.assertNotEquals(MockEventBusService.radiusOperstate, 0);
        log.info("testAuthenticationStatisticsEvent: executed successfully");
    }

    @Test
    void testAaaMachineStatisticsEvent() {
        AaaMachineStatisticsEvent event = new AaaMachineStatisticsEvent(AaaMachineStatisticsEvent.Type.STATS_UPDATE,
                new AaaSupplicantMachineStats());
        aaaMachineStatisticsEventListener.event(event);
        Assert.assertNotEquals(MockEventBusService.authStatsCounter, 0);
        log.info("testAuthenticationStatisticsEvent: executed successfully");
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
            return new MockAnnotations();
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
        static int authCounter = 0;
        static int authStatsCounter = 0;
        static int radiusOperstate = 0;

        @Override
        public void send(String topic, JsonNode data) {
            switch (topic) {
                case "authentication.events":
                    authCounter++;
                    log.info("Authenticated successfully");
                    break;
                case "onos.aaa.stats.kpis":
                    authStatsCounter++;
                    break;
                case "radiusOperationalStatus.events":
                    radiusOperstate++;
                    break;

                default:
                    break;
            }
        }
    }

    /*
    Mocks Device Service Adapter.
     */
    private class MockDeviceService extends DeviceServiceAdapter {

        private final ProviderId providerId = new ProviderId("of", "foo");
        private final Device device1 = new MockDevice(providerId, DEVICE_ID_1, Device.Type.SWITCH,
                "foo.inc", "0", "0", OLT_DEV_ID, new ChassisId(),
                DEVICE_ANNOTATIONS);

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
            return new MockPort();
        }

        @Override
        public Port getPort(DeviceId deviceId, PortNumber portNumber) {
            return new MockPort();
        }

        @Override
        public List<Port> getPorts(DeviceId deviceId) {
            return Lists.newArrayList(new MockPort());
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

    private class MockAuthenticationService implements AuthenticationService {
        @Override
        public Iterable<AuthenticationRecord> getAuthenticationRecords() {
            return null;
        }

        @Override
        public boolean removeAuthenticationStateByMac(MacAddress macAddress) {
            return false;
        }

        @Override
        public AaaSupplicantMachineStats getSupplicantMachineStats(String s) {
            return null;
        }

        @Override
        public void addListener(AuthenticationEventListener listener) {
            authEventListener = listener;
        }

        @Override
        public void removeListener(AuthenticationEventListener listener) {

        }
    }

    private class MockAuthenticationStatisticsService implements AuthenticationStatisticsService {
        @Override
        public AaaStatistics getAaaStats() {
            return null;
        }

        @Override
        public AaaStatisticsSnapshot getClusterStatistics() {
            return null;
        }

        @Override
        public void handleRoundtripTime(byte b) {

        }

        @Override
        public void calculatePacketRoundtripTime() {

        }

        @Override
        public void putOutgoingIdentifierToMap(byte b) {

        }

        @Override
        public void resetAllCounters() {

        }

        @Override
        public void addListener(AuthenticationStatisticsEventListener listener) {
            authStatisticsEventListener = listener;
        }

        @Override
        public void removeListener(AuthenticationStatisticsEventListener listener) {

        }
    }

    private class MockRadiusOperationalStatusService implements RadiusOperationalStatusService {
        @Override
        public RadiusOperationalStatusEventDelegate getRadiusOprStDelegate() {
            return null;
        }

        @Override
        public String getRadiusServerOperationalStatus() {
            return null;
        }

        @Override
        public void setStatusServerReqSent(boolean b) {

        }

        @Override
        public void setRadiusOperationalStatusEvaluationMode(
                RadiusOperationalStatusService.RadiusOperationalStatusEvaluationMode
                        radiusOperationalStatusEvaluationMode) {

        }

        @Override
        public void setOperationalStatusServerTimeoutInMillis(long l) {

        }

        @Override
        public void checkServerOperationalStatus() {

        }

        @Override
        public boolean isRadiusResponseForOperationalStatus(byte b) {
            return false;
        }

        @Override
        public void handleRadiusPacketForOperationalStatus(RADIUS radius) {

        }

        @Override
        public void initialize(byte[] bytes, String s, RadiusCommunicator radiusCommunicator) {

        }

        @Override
        public void setOutTimeInMillis(byte b) {

        }

        @Override
        public void addListener(RadiusOperationalStatusEventListener listener) {
            radiusOperStatusEventListener = listener;
        }

        @Override
        public void removeListener(RadiusOperationalStatusEventListener listener) {

        }
    }

    private class MockAaaMachineStatisticsService implements AaaMachineStatisticsService {
        @Override
        public AaaSupplicantMachineStats getSupplicantStats(Object o) {
            return null;
        }

        @Override
        public AaaMachineStatisticsDelegate getMachineStatsDelegate() {
            return null;
        }

        @Override
        public void logAaaSupplicantMachineStats(AaaSupplicantMachineStats aaaSupplicantMachineStats) {

        }

        @Override
        public void addListener(AaaMachineStatisticsEventListener listener) {
            aaaMachineStatisticsEventListener = listener;
        }

        @Override
        public void removeListener(AaaMachineStatisticsEventListener listener) {

        }
    }
}