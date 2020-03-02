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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.onlab.packet.IpAddress;
import org.opencord.cordmcast.CordMcastStatistics;
import org.opencord.cordmcast.CordMcastStatisticsEvent;
import org.opencord.cordmcast.CordMcastStatisticsEventListener;
import org.opencord.cordmcast.CordMcastStatisticsService;
import org.opencord.kafka.EventBusService;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Deactivate;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Component(immediate = true)
public class McastKafkaIntegration extends AbstractKafkaIntegration {

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected EventBusService eventBusService;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL,
            policy = ReferencePolicy.DYNAMIC,
            bind = "bindMcastStatisticsService",
            unbind = "unbindMcastStatisticsService")
    protected volatile CordMcastStatisticsService ignore1;
    protected final AtomicReference<CordMcastStatisticsService> cordMcastStatisticsServiceRef = new AtomicReference<>();

    private final CordMcastStatisticsEventListener cordMcastStatisticsEventListener =
            new InternalCorcMcastStatisticsListener();

    private static final String MCAST_OPERATIONAL_STATUS_TOPIC = "mcastOperationalStatus.events";

    //cord mcast stats event params
    private static final String GROUP = "group";
    private static final String SOURCE = "source";
    private static final String VLAN = "vlan";

    protected void bindMcastStatisticsService(CordMcastStatisticsService cordMcastStatisticsService) {
        bindAndAddListener(cordMcastStatisticsService, cordMcastStatisticsServiceRef, cordMcastStatisticsEventListener);
    }

    protected void unbindMcastStatisticsService(CordMcastStatisticsService cordMcastStatisticsService) {
        unbindAndRemoveListener(cordMcastStatisticsService,
                cordMcastStatisticsServiceRef, cordMcastStatisticsEventListener);
    }

    @Activate
    public void activate() {
        log.info("Started McastKafkaIntegration");
    }

    @Deactivate
    public void deactivate() {
        unbindMcastStatisticsService(cordMcastStatisticsServiceRef.get());
        log.info("Stopped McastKafkaIntegration");
    }

    private void handleMcastStat(CordMcastStatisticsEvent mcastStatEvent) {
        eventBusService.send(MCAST_OPERATIONAL_STATUS_TOPIC, serializeMcastStat(mcastStatEvent));
        log.info("CordMcastStatisticsEvent sent successfully");
    }

    private JsonNode serializeMcastStat(CordMcastStatisticsEvent mcastStatEvent) {
        log.debug("Serializing AuthenticationStatisticsEvent");
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode mcastStat = mapper.createObjectNode();
        Map<IpAddress, CordMcastStatistics> subject = mcastStatEvent.subject();
        HashMap<IpAddress, CordMcastStatistics> map = (HashMap<IpAddress, CordMcastStatistics>) subject;
        map.forEach((k, v) -> {
            log.debug("Group: " + k + " | Source: " + v.getSourceAddress() + " | Vlan: " + v.getVlanId().toString());
            mcastStat.put("Group: " + k.toString(),
                    " | Source: " + v.getSourceAddress().toString() + " | vlan: " + v.getVlanId().toString());
        });

        return mcastStat;
    }

    private class InternalCorcMcastStatisticsListener implements CordMcastStatisticsEventListener {

        @Override
        public void event(CordMcastStatisticsEvent mcastStatEvent) {
            handleMcastStat(mcastStatEvent);
        }
    }
}
