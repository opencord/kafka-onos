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
import org.onlab.packet.MacAddress;
import org.onlab.packet.RADIUS;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.PortNumber;
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

import static org.opencord.kafka.integrations.MockDeviceService.DEVICE_ID_1;

class AaaKafkaIntegrationTest {

    private AaaKafkaIntegration aaaKafkaInt;
    private AuthenticationEventListener authEventListener;
    private AuthenticationStatisticsEventListener authStatisticsEventListener;
    private RadiusOperationalStatusEventListener radiusOperStatusEventListener;
    private AaaMachineStatisticsEventListener aaaMachineStatisticsEventListener;

    private static final String TOPIC = "authentication.events";
    private static final String AUTHENTICATION_STATISTICS_TOPIC = "onos.aaa.stats.kpis";
    private static final String RADIUS_OPERATION_STATUS_TOPIC = "radiusOperationalStatus.events";

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
    }

    @Test
    void testAuthenticationStatisticsEvent() {
        AuthenticationStatisticsEvent event = new AuthenticationStatisticsEvent(
                AuthenticationStatisticsEvent.Type.STATS_UPDATE,
                new AaaStatistics());
        authStatisticsEventListener.event(event);
        Assert.assertNotEquals(MockEventBusService.authStatsCounter, 0);
    }

    @Test
    void testRadiusOperationalStatusEvent() {
        RadiusOperationalStatusEvent event = new RadiusOperationalStatusEvent(
                RadiusOperationalStatusEvent.Type.RADIUS_OPERATIONAL_STATUS,
                "AUTHENTICATED");
        radiusOperStatusEventListener.event(event);
        Assert.assertNotEquals(MockEventBusService.radiusOperstate, 0);
    }

    @Test
    void testAaaMachineStatisticsEvent() {
        AaaMachineStatisticsEvent event = new AaaMachineStatisticsEvent(AaaMachineStatisticsEvent.Type.STATS_UPDATE,
                new AaaSupplicantMachineStats());
        aaaMachineStatisticsEventListener.event(event);
        Assert.assertNotEquals(MockEventBusService.authStatsCounter, 0);
    }


    private static class MockEventBusService implements EventBusService {
        static int authCounter = 0;
        static int authStatsCounter = 0;
        static int radiusOperstate = 0;

        @Override
        public void send(String topic, JsonNode data) {
            switch (topic) {
                case TOPIC:
                    authCounter++;
                    break;
                case AUTHENTICATION_STATISTICS_TOPIC:
                    authStatsCounter++;
                    break;
                case RADIUS_OPERATION_STATUS_TOPIC:
                    radiusOperstate++;
                    break;

                default:
                    break;
            }
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