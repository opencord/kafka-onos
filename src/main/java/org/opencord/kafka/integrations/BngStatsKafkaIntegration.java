/*
 * Copyright 2019-present Open Networking Foundation
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
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.onosproject.net.behaviour.BngProgrammable;
import org.onosproject.net.pi.runtime.PiCounterCellData;
import org.opencord.bng.BngStatsEvent;
import org.opencord.bng.BngStatsEventListener;
import org.opencord.bng.BngStatsEventSubject;
import org.opencord.bng.BngStatsService;
import org.opencord.kafka.EventBusService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Listens for statistic events from the BNG app and pushes them on a Kafka
 * bus.
 */
@Component(immediate = true)
public class BngStatsKafkaIntegration extends AbstractKafkaIntegration {

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected EventBusService eventBusService;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL,
            policy = ReferencePolicy.DYNAMIC,
            bind = "bindBngStatsService",
            unbind = "unbindBngStatsService")
    protected volatile BngStatsService ignore;
    private final AtomicReference<BngStatsService> bngStatsServiceRef = new AtomicReference<>();

    protected static final String TOPIC_STATS = "bng.stats";
    private static final String SUBSCRIBER_S_TAG = "sTag";
    private static final String SUBSCRIBER_C_TAG = "cTag";

    private static final String UP_TX_BYTES = "upTxBytes";
    private static final String UP_TX_PACKETS = "upTxPackets";

    private static final String UP_RX_BYTES = "upRxBytes";
    private static final String UP_RX_PACKETS = "upRxPackets";

    private static final String UP_DROP_BYTES = "upDropBytes";
    private static final String UP_DROP_PACKETS = "upDropPackets";

    private static final String DOWN_RX_BYTES = "downRxBytes";
    private static final String DOWN_RX_PACKETS = "downRxPackets";

    private static final String DOWN_TX_BYTES = "downTxBytes";
    private static final String DOWN_TX_PACKETS = "downTxPackets";

    private static final String DOWN_DROP_BYTES = "downDropBytes";
    private static final String DOWN_DROP_PACKETS = "downDropPackets";

    private static final String CONTROL_PACKETS = "controlPackets";

    private static final ImmutableMap<BngProgrammable.BngCounterType, Pair<String, String>> MAP_COUNTERS =
            ImmutableMap.<BngProgrammable.BngCounterType, Pair<String, String>>builder()
                    .put(BngProgrammable.BngCounterType.UPSTREAM_RX, Pair.of(UP_RX_BYTES, UP_RX_PACKETS))
                    .put(BngProgrammable.BngCounterType.UPSTREAM_TX, Pair.of(UP_TX_BYTES, UP_TX_PACKETS))
                    .put(BngProgrammable.BngCounterType.UPSTREAM_DROPPED, Pair.of(UP_DROP_BYTES, UP_DROP_PACKETS))

                    .put(BngProgrammable.BngCounterType.DOWNSTREAM_RX, Pair.of(DOWN_RX_BYTES, DOWN_RX_PACKETS))
                    .put(BngProgrammable.BngCounterType.DOWNSTREAM_TX, Pair.of(DOWN_TX_BYTES, DOWN_TX_PACKETS))
                    .put(BngProgrammable.BngCounterType.DOWNSTREAM_DROPPED, Pair.of(DOWN_DROP_BYTES, DOWN_DROP_PACKETS))
                    .build();

    private static final String TIMESTAMP = "timestamp";
    private static final String ATTACHMENT_TYPE = "attachmentType";
    private static final String DEVICE_ID = "deviceId";
    private static final String PORT_NUMBER = "portNumber";
    private static final String MAC_ADDRESS = "macAddress";
    private static final String IP_ADDRESS = "ipAddress";
    private static final String ONU_SERIAL_NUMBER = "onuSerialNumber";
    private static final String PPPOE_SESSION_ID = "pppoeSessionId";

    private final BngStatsEventListener statsListener = new InternalStatsListener();

    protected void bindBngStatsService(BngStatsService incomingService) {
        bindAndAddListener(incomingService, bngStatsServiceRef, statsListener);
    }

    protected void unbindBngStatsService(BngStatsService outgoingService) {
        unbindAndRemoveListener(outgoingService, bngStatsServiceRef, statsListener);
    }

    @Activate
    public void activate() {
        log.info("Started BngKafkaIntegration");
    }

    @Deactivate
    public void deactivate() {
        unbindBngStatsService(bngStatsServiceRef.get());
        log.info("Stopped BngKafkaIntegration");
    }

    private JsonNode serializeBngStatsEvent(BngStatsEventSubject eventSubject) {
        // Serialize stats in a JSON node
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode attStatsNode = mapper.createObjectNode();

        // Exposing statistics only for PPPoE attachment
        var attachmentStats = eventSubject.getAttachmentStats();
        var attachment = eventSubject.getBngAttachment();

        attStatsNode.put(MAC_ADDRESS, attachment.macAddress().toString());
        attStatsNode.put(IP_ADDRESS, attachment.ipAddress().toString());
        attStatsNode.put(PPPOE_SESSION_ID, attachment.pppoeSessionId());

        attStatsNode.put(SUBSCRIBER_S_TAG, attachment.sTag().toShort());
        attStatsNode.put(SUBSCRIBER_C_TAG, attachment.cTag().toShort());

        attStatsNode.put(ONU_SERIAL_NUMBER, attachment.onuSerial());
        attStatsNode.put(ATTACHMENT_TYPE, attachment.type().toString());

        attStatsNode.put(DEVICE_ID, attachment.oltConnectPoint().deviceId().toString());
        attStatsNode.put(PORT_NUMBER, attachment.oltConnectPoint().port().toString());

        // Add the statistics to the JSON
        attStatsNode = createNodesStats(attachmentStats, attStatsNode);

        // Control stats are different, only packets statistics
        attStatsNode.put(CONTROL_PACKETS,
                attachmentStats.get(BngProgrammable.BngCounterType.CONTROL_PLANE).packets());

        attStatsNode.put(TIMESTAMP, Instant.now().toString());
        return attStatsNode;
    }

    private ObjectNode createNodesStats(Map<BngProgrammable.BngCounterType,
            PiCounterCellData> attStats, ObjectNode node) {
        MAP_COUNTERS.forEach((counterType, pairStats) -> {
            if (attStats.containsKey(counterType)) {
                node.put(pairStats.getLeft(),
                        attStats.get(counterType).bytes());
                node.put(pairStats.getRight(),
                        attStats.get(counterType).packets());
            }
        });
        return node;
    }

    private class InternalStatsListener implements BngStatsEventListener {

        @Override
        public void event(BngStatsEvent event) {
            eventBusService.send(TOPIC_STATS, serializeBngStatsEvent(event.subject()));
        }
    }
}
