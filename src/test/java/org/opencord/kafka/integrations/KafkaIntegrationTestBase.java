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

import com.google.common.collect.Lists;
import org.onlab.packet.IpAddress;
import org.onlab.packet.MacAddress;
import org.onlab.packet.VlanId;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.Port;
import org.onosproject.net.PortNumber;
import org.onosproject.net.behaviour.BngProgrammable;
import org.onosproject.net.pi.runtime.PiCounterCellData;
import org.opencord.aaa.AaaMachineStatisticsEvent;
import org.opencord.aaa.AaaStatistics;
import org.opencord.aaa.AaaSupplicantMachineStats;
import org.opencord.aaa.AuthenticationEvent;
import org.opencord.aaa.AuthenticationStatisticsEvent;
import org.opencord.aaa.RadiusOperationalStatusEvent;
import org.opencord.bng.BngStatsEvent;
import org.opencord.bng.BngStatsEventSubject;
import org.opencord.bng.PppoeBngAttachment;
import org.opencord.bng.PppoeEvent;
import org.opencord.bng.PppoeEventSubject;
import org.opencord.cordmcast.CordMcastStatistics;
import org.opencord.cordmcast.CordMcastStatisticsEvent;
import org.opencord.igmpproxy.IgmpStatistics;
import org.opencord.igmpproxy.IgmpStatisticsEvent;
import org.opencord.olt.AccessDeviceEvent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencord.kafka.integrations.MockDeviceService.DEVICE_ID_1;

/**
 * Base class for the KafkaIntegration tests classes.
 */
public class KafkaIntegrationTestBase {
    protected static final PortNumber PORT_NUMBER = PortNumber.portNumber(1);
    protected static final Port PORT = new MockDeviceService.MockPort();
    protected static final VlanId CLIENT_C_TAG = VlanId.vlanId((short) 999);
    protected static final VlanId CLIENT_S_TAG = VlanId.vlanId((short) 111);
    protected static final Short TPID = 8;
    protected static final VlanId C_TAG = VlanId.vlanId((short) 999);
    protected static final VlanId S_TAG = VlanId.vlanId((short) 111);
    protected static final String ONU_SERIAL = "TWSH00008084";
    protected static final IpAddress LOCAL_IP = IpAddress.valueOf("127.0.0.1");
    protected static final MacAddress OLT_MAC = MacAddress.valueOf("c6:b1:cd:40:dc:93");
    protected static final Short SESSION_ID = 2;
    protected static final ConnectPoint OLT_CONNECT_POINT = new ConnectPoint(MockDeviceService.DEVICE_ID_1,
            PORT_NUMBER);

    protected AuthenticationEvent getAuthenticationEvent() {
        return new AuthenticationEvent(AuthenticationEvent.Type.APPROVED,
                OLT_CONNECT_POINT);
    }

    protected AuthenticationStatisticsEvent getAuthenticationStatisticsEvent() {
        return new AuthenticationStatisticsEvent(
                AuthenticationStatisticsEvent.Type.STATS_UPDATE,
                new AaaStatistics());
    }

    protected RadiusOperationalStatusEvent getRadiusOperationalStatusEvent() {
        return new RadiusOperationalStatusEvent(
                RadiusOperationalStatusEvent.Type.RADIUS_OPERATIONAL_STATUS,
                "AUTHENTICATED");
    }

    protected AaaMachineStatisticsEvent getAaaMachineStatisticsEvent() {
        return new AaaMachineStatisticsEvent(AaaMachineStatisticsEvent.Type.STATS_UPDATE,
                new AaaSupplicantMachineStats());
    }

    protected AccessDeviceEvent getUniAdded() {
        return new AccessDeviceEvent(AccessDeviceEvent.Type.UNI_ADDED,
                DEVICE_ID_1, PORT, CLIENT_S_TAG, CLIENT_C_TAG, (int) TPID);
    }

    protected AccessDeviceEvent getUniRemoved() {
        return new AccessDeviceEvent(AccessDeviceEvent.Type.UNI_REMOVED,
                DEVICE_ID_1, PORT, CLIENT_S_TAG, CLIENT_C_TAG, (int) TPID);
    }

    protected PppoeEvent getPppoeEvent() {
        return new PppoeEvent(PppoeEvent.EventType.AUTH_SUCCESS,
                new PppoeEventSubject(OLT_CONNECT_POINT,
                        LOCAL_IP, OLT_MAC,
                        ONU_SERIAL, SESSION_ID, S_TAG, C_TAG));
    }

    protected BngStatsEvent getBngStatsEvent() {
        PppoeBngAttachment pppoeBngAttachment = (PppoeBngAttachment) PppoeBngAttachment.builder()
                .withPppoeSessionId(SESSION_ID)
                .withCTag(C_TAG)
                .withIpAddress(LOCAL_IP)
                .withMacAddress(OLT_MAC)
                .withOltConnectPoint(OLT_CONNECT_POINT)
                .withOnuSerial(ONU_SERIAL)
                .withQinqTpid(TPID)
                .withSTag(S_TAG)
                .build();
        Map<BngProgrammable.BngCounterType, PiCounterCellData> attachmentStats = new HashMap<>();
        attachmentStats.put(BngProgrammable.BngCounterType.CONTROL_PLANE,
                new PiCounterCellData(1024, 1024));
        BngStatsEventSubject subject = new BngStatsEventSubject("PppoeKey",
                pppoeBngAttachment, attachmentStats);
        return new BngStatsEvent(BngStatsEvent.EventType.STATS_UPDATED, subject);
    }

    protected IgmpStatisticsEvent getIgmpStatisticsEvent() {
        return new IgmpStatisticsEvent(
                IgmpStatisticsEvent.Type.STATS_UPDATE, new IgmpStatistics());
    }

    protected CordMcastStatisticsEvent getCordMcastStatisticsEvent() {
        List<CordMcastStatistics> statsList = Lists.newArrayList(
                new CordMcastStatistics(IpAddress.valueOf("172.16.34.34"),
                        "192.168.0.21", VlanId.vlanId("100")),
                new CordMcastStatistics(IpAddress.valueOf("172.16.35.35"),
                        "192.168.0.22", VlanId.vlanId("101"))
        );
        return new CordMcastStatisticsEvent(
                CordMcastStatisticsEvent.Type.STATUS_UPDATE, statsList);
    }
}
