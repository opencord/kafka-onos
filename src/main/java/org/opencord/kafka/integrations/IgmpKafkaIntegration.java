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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opencord.igmpproxy.IgmpStatisticType;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.opencord.kafka.EventBusService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

import org.opencord.igmpproxy.IgmpStatisticsEvent;
import org.opencord.igmpproxy.IgmpStatisticsEventListener;
import org.opencord.igmpproxy.IgmpStatisticsService;

/**
 * Listens for IGMP events and pushes them on a Kafka bus.
 */

@Component(immediate = true)
public class IgmpKafkaIntegration extends AbstractKafkaIntegration {

    public Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected EventBusService eventBusService;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL,
            policy = ReferencePolicy.DYNAMIC,
            bind = "bindIgmpStatService",
            unbind = "unbindIgmpStatService")
    protected volatile IgmpStatisticsService ignore;
    private final AtomicReference<IgmpStatisticsService> igmpStatServiceRef = new AtomicReference<>();

    private final IgmpStatisticsEventListener igmpStatisticsEventListener =
            new InternalIgmpStatisticsListener();

    //TOPIC
    protected static final String IGMP_STATISTICS_TOPIC = "onos.igmp.stats.kpis";

    // IGMP stats event params
    private static final String IGMP_JOIN_REQ = "igmpJoinReq";
    private static final String IGMP_SUCCESS_JOIN_REJOIN_REQ = "igmpSuccessJoinRejoinReq";
    private static final String IGMP_FAIL_JOIN_REQ = "igmpFailJoinReq";
    private static final String IGMP_LEAVE_REQ = "igmpLeaveReq";
    private static final String IGMP_DISCONNECT = "igmpDisconnect";
    private static final String IGMP_V3_MEMBERSHIP_QUERY = "igmpv3MembbershipQuery";
    private static final String IGMP_V1_MEMBERSHIP_REPORT = "igmpv1MembershipReport";
    private static final String IGMP_V2_MEMBERSHIP_REPORT = "igmpv2MembershipReport";
    private static final String IGMP_V3_MEMBERSHIP_REPORT = "igmpv3MembershipReport";
    private static final String IGMP_V2_LEAVE_GROUP = "igmpv2LeaveGroup";
    private static final String TOTAL_MSG_RECEIVED = "totalMsgReceived";
    private static final String IGMP_MSG_RECEIVED = "igmpMsgReceived";
    private static final String INVALID_IGMP_MSG_RECEIVED = "invalidIgmpMsgReceived";

    private static final String UNKNOWN_IGMP_TYPE_PACKETS_RX_COUNTER =
            "unknownIgmpTypePacketsRxCounter";
    private static final String REPORTS_RX_WITH_WRONG_MODE_COUNTER =
            "reportsRxWithWrongModeCounter";
    private static final String FAIL_JOIN_REQ_INSUFF_PERMISSION_ACCESS_COUNTER =
            "failJoinReqInsuffPermissionAccessCounter";
    private static final String FAIL_JOIN_REQ_UNKNOWN_MULTICAST_IP_COUNTER =
            "failJoinReqUnknownMulticastIpCounter";
    private static final String UNCONFIGURED_GROUP_COUNTER =
            "unconfiguredGroupCounter";
    private static final String VALID_IGMP_PACKET_COUNTER =
            "validIgmpPacketCounter";
    private static final String IGMP_CHANNEL_JOIN_COUNTER =
            "igmpChannelJoinCounter";
    private static final String CURRENT_GRP_NUM_COUNTER =
            "currentGrpNumCounter";
    private static final String IGMP_VALID_CHECKSUM_COUNTER =
            "igmpValidChecksumCounter";
    private static final String INVALID_IGMP_LENGTH =
            "invalidIgmpLength";
    private static final String IGMP_GENERAL_MEMBERSHIP_QUERY =
            "igmpGeneralMembershipQuery";
    private static final String IGMP_GRP_SPECIFIC_MEMBERSHIP_QUERY =
            "igmpGrpSpecificMembershipQuery";
    private static final String IGMP_GRP_AND_SRC_SPECIFIC_MEMBERSHIP_QUERY =
            "igmpGrpAndSrcSpecificMembershipQuery";

    protected void bindIgmpStatService(IgmpStatisticsService incomingService) {
        bindAndAddListener(incomingService, igmpStatServiceRef, igmpStatisticsEventListener);
    }

    protected void unbindIgmpStatService(IgmpStatisticsService outgoingService) {
        unbindAndRemoveListener(outgoingService, igmpStatServiceRef, igmpStatisticsEventListener);
    }

    @Activate
    public void activate() {
        log.info("Started IgmpKafkaIntegration");
    }

    @Deactivate
    public void deactivate() {
        log.info("Stopped IgmpKafkaIntegration");
    }

    private void handleStat(IgmpStatisticsEvent event) {
        eventBusService.send(IGMP_STATISTICS_TOPIC, serializeStat(event));
        if (log.isTraceEnabled()) {
            log.trace("IGMPStatisticsEvent sent successfully");
        }
    }

    private JsonNode serializeStat(IgmpStatisticsEvent event) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode igmpStatEvent = mapper.createObjectNode();
        igmpStatEvent.put(IGMP_JOIN_REQ, event.subject()
                .getStat(IgmpStatisticType.IGMP_JOIN_REQ));
        igmpStatEvent.put(IGMP_SUCCESS_JOIN_REJOIN_REQ, event.subject()
                .getStat(IgmpStatisticType.IGMP_SUCCESS_JOIN_RE_JOIN_REQ));
        igmpStatEvent.put(IGMP_FAIL_JOIN_REQ, event.subject()
                .getStat(IgmpStatisticType.IGMP_FAIL_JOIN_REQ));
        igmpStatEvent.put(IGMP_LEAVE_REQ, event.subject()
                .getStat(IgmpStatisticType.IGMP_LEAVE_REQ));
        igmpStatEvent.put(IGMP_DISCONNECT, event.subject()
                .getStat(IgmpStatisticType.IGMP_DISCONNECT));
        igmpStatEvent.put(IGMP_V3_MEMBERSHIP_QUERY, event.subject()
                .getStat(IgmpStatisticType.IGMP_V3_MEMBERSHIP_QUERY));
        igmpStatEvent.put(IGMP_V1_MEMBERSHIP_REPORT, event.subject()
                .getStat(IgmpStatisticType.IGMP_V1_MEMBERSHIP_REPORT));
        igmpStatEvent.put(IGMP_V2_MEMBERSHIP_REPORT, event.subject()
                .getStat(IgmpStatisticType.IGMP_V2_MEMBERSHIP_REPORT));
        igmpStatEvent.put(IGMP_V3_MEMBERSHIP_REPORT, event.subject()
                .getStat(IgmpStatisticType.IGMP_V3_MEMBERSHIP_REPORT));
        igmpStatEvent.put(IGMP_V2_LEAVE_GROUP, event.subject()
                .getStat(IgmpStatisticType.IGMP_V2_LEAVE_GROUP));
        igmpStatEvent.put(TOTAL_MSG_RECEIVED, event.subject()
                .getStat(IgmpStatisticType.TOTAL_MSG_RECEIVED));
        igmpStatEvent.put(IGMP_MSG_RECEIVED, event.subject()
                .getStat(IgmpStatisticType.IGMP_MSG_RECEIVED));
        igmpStatEvent.put(INVALID_IGMP_MSG_RECEIVED, event.subject()
                .getStat(IgmpStatisticType.INVALID_IGMP_MSG_RECEIVED));
        igmpStatEvent.put(UNKNOWN_IGMP_TYPE_PACKETS_RX_COUNTER, event.subject()
                .getStat(IgmpStatisticType.UNKNOWN_IGMP_TYPE_PACKETS_RX_COUNTER));
        igmpStatEvent.put(REPORTS_RX_WITH_WRONG_MODE_COUNTER, event.subject()
                .getStat(IgmpStatisticType.REPORTS_RX_WITH_WRONG_MODE_COUNTER));
        igmpStatEvent.put(FAIL_JOIN_REQ_INSUFF_PERMISSION_ACCESS_COUNTER,
                event.subject().getStat(IgmpStatisticType.FAIL_JOIN_REQ_INSUFF_PERMISSION_ACCESS_COUNTER));
        igmpStatEvent.put(FAIL_JOIN_REQ_UNKNOWN_MULTICAST_IP_COUNTER,
                event.subject().getStat(IgmpStatisticType.FAIL_JOIN_REQ_UNKNOWN_MULTICAST_IP_COUNTER));
        igmpStatEvent.put(UNCONFIGURED_GROUP_COUNTER, event.subject()
                .getStat(IgmpStatisticType.UNCONFIGURED_GROUP_COUNTER));
        igmpStatEvent.put(VALID_IGMP_PACKET_COUNTER, event.subject()
                .getStat(IgmpStatisticType.VALID_IGMP_PACKET_COUNTER));
        igmpStatEvent.put(IGMP_CHANNEL_JOIN_COUNTER, event.subject()
                .getStat(IgmpStatisticType.IGMP_CHANNEL_JOIN_COUNTER));
        igmpStatEvent.put(CURRENT_GRP_NUM_COUNTER, event.subject()
                .getStat(IgmpStatisticType.CURRENT_GRP_NUMBER_COUNTER));
        igmpStatEvent.put(IGMP_VALID_CHECKSUM_COUNTER, event.subject()
                .getStat(IgmpStatisticType.IGMP_VALID_CHECKSUM_COUNTER));
        igmpStatEvent.put(INVALID_IGMP_LENGTH, event.subject()
                .getStat(IgmpStatisticType.INVALID_IGMP_LENGTH));
        igmpStatEvent.put(IGMP_GENERAL_MEMBERSHIP_QUERY, event.subject()
                .getStat(IgmpStatisticType.IGMP_GENERAL_MEMBERSHIP_QUERY));
        igmpStatEvent.put(IGMP_GRP_SPECIFIC_MEMBERSHIP_QUERY, event.subject()
                .getStat(IgmpStatisticType.IGMP_GRP_SPECIFIC_MEMBERSHIP_QUERY));
        igmpStatEvent.put(IGMP_GRP_AND_SRC_SPECIFIC_MEMBERSHIP_QUERY,
                event.subject().getStat(IgmpStatisticType.IGMP_GRP_AND_SRC_SPESIFIC_MEMBERSHIP_QUERY));
        return igmpStatEvent;
    }

    public class InternalIgmpStatisticsListener implements
            IgmpStatisticsEventListener {

        @Override
        public void event(IgmpStatisticsEvent igmpStatisticsEvent) {
            handleStat(igmpStatisticsEvent);
        }
    }
}
