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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.onosproject.net.behaviour.BngProgrammable;
import org.opencord.bng.BngStatsEvent;
import org.opencord.bng.BngStatsEventListener;
import org.opencord.bng.BngStatsEventSubject;
import org.opencord.bng.BngStatsService;
import org.opencord.bng.PppoeAttachment;
import org.opencord.kafka.EventBusService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;

import java.time.Instant;

/**
 * Listens for statistic events from the BNG app and pushes them on a Kafka
 * bus.
 */
@Component(immediate = true)
public class BngStatsKafkaIntegration extends AbstractKafkaIntegration {

    private static final String TOPIC_STATS = "bng.stats";
    private static final String ATTACHMENTS = "attachments";
    private static final String SUBSCRIBER_S_TAG = "sTag";
    private static final String SUBSCRIBER_C_TAG = "cTag";

    private static final String UP_TX_BYTES = "upTermBytes";
    private static final String UP_TX_PACKETS = "upTermPackets";

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

    private static final String TIMESTAMP = "timestamp";
    private static final String ATTACHMENT_TYPE = "attachmentType";
    private static final String DEVICE_ID = "deviceId";
    private static final String PORT_NUMBER = "portNumber";
    private static final String MAC_ADDRESS = "macAddress";
    private static final String IP_ADDRESS = "ipAddress";
    private static final String ONU_SERIAL_NUMBER = "onuSerialNumber";
    private static final String PPPOE_SESSION_ID = "pppoeSessionId";


    private final BngStatsEventListener statsListener = new InternalStatsListener();

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected EventBusService eventBusService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected BngStatsService bngStatsService;

    @Activate
    public void activate() {
        bngStatsService.addListener(statsListener);
        log.info("Started BngKafkaIntegration");
    }

    @Deactivate
    public void deactivate() {
        bngStatsService.removeListener(statsListener);
        log.info("Stopped BngKafkaIntegration");
    }

    private JsonNode serializeBngStatsEvent(BngStatsEventSubject eventSubject) {
        // Serialize stats in a JSON node
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode bnguStats = mapper.createObjectNode();
        ArrayNode subscribers = mapper.createArrayNode();

        // Exposing statistics only for PPPoE attachment
        eventSubject.getStats().entrySet().stream().
                filter(attachmentMapEntry ->
                    attachmentMapEntry.getKey().type() == BngProgrammable.Attachment.AttachmentType.PPPoE)
                .forEach(attachmentMapEntry -> {
                    var att = (PppoeAttachment) attachmentMapEntry.getKey();
                    var stats = attachmentMapEntry.getValue();
                    ObjectNode sub = mapper.createObjectNode();

                    sub.put(MAC_ADDRESS, att.macAddress().toString());
                    sub.put(IP_ADDRESS, att.ipAddress().toString());
                    sub.put(PPPOE_SESSION_ID, att.pppoeSessionId());

                    sub.put(SUBSCRIBER_S_TAG, att.sTag().toShort());
                    sub.put(SUBSCRIBER_C_TAG, att.cTag().toShort());

                    sub.put(ONU_SERIAL_NUMBER, att.onuSerial());
                    sub.put(ATTACHMENT_TYPE, att.type().toString());

                    sub.put(DEVICE_ID, att.oltConnectPoint().deviceId().toString());
                    sub.put(PORT_NUMBER, att.oltConnectPoint().port().toString());

                    sub.put(UP_TX_BYTES, stats.get(BngProgrammable.BngCounterType.UPSTREAM_TX).bytes());
                    sub.put(UP_TX_PACKETS, stats.get(BngProgrammable.BngCounterType.UPSTREAM_TX).packets());

                    sub.put(UP_RX_BYTES, stats.get(BngProgrammable.BngCounterType.UPSTREAM_RX).bytes());
                    sub.put(UP_RX_PACKETS, stats.get(BngProgrammable.BngCounterType.UPSTREAM_RX).packets());

                    sub.put(UP_DROP_BYTES, stats.get(BngProgrammable.BngCounterType.UPSTREAM_DROPPED).bytes());
                    sub.put(UP_DROP_PACKETS, stats.get(BngProgrammable.BngCounterType.UPSTREAM_DROPPED).packets());

                    sub.put(DOWN_RX_BYTES, stats.get(BngProgrammable.BngCounterType.DOWNSTREAM_RX).bytes());
                    sub.put(DOWN_RX_PACKETS, stats.get(BngProgrammable.BngCounterType.DOWNSTREAM_RX).packets());

                    sub.put(DOWN_TX_BYTES, stats.get(BngProgrammable.BngCounterType.DOWNSTREAM_TX).bytes());
                    sub.put(DOWN_TX_PACKETS, stats.get(BngProgrammable.BngCounterType.DOWNSTREAM_TX).packets());

                    sub.put(DOWN_DROP_BYTES, stats.get(BngProgrammable.BngCounterType.DOWNSTREAM_DROPPED).bytes());
                    sub.put(DOWN_DROP_PACKETS, stats.get(BngProgrammable.BngCounterType.DOWNSTREAM_DROPPED).packets());

                    sub.put(CONTROL_PACKETS, stats.get(BngProgrammable.BngCounterType.CONTROL_PLANE).packets());
                    subscribers.add(sub);
                });
        bnguStats.put(ATTACHMENTS, subscribers);
        bnguStats.put(TIMESTAMP, Instant.now().toString());
        return bnguStats;
    }

    private class InternalStatsListener implements BngStatsEventListener {

        @Override
        public void event(BngStatsEvent event) {
            eventBusService.send(TOPIC_STATS, serializeBngStatsEvent(event.subject()));
        }
    }
}
