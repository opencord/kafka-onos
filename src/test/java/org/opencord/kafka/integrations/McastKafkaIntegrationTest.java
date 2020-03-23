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
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.onlab.packet.IpAddress;
import org.onlab.packet.VlanId;
import org.opencord.cordmcast.CordMcastStatistics;
import org.opencord.cordmcast.CordMcastStatisticsEvent;
import org.opencord.cordmcast.CordMcastStatisticsEventListener;
import org.opencord.cordmcast.CordMcastStatisticsService;
import org.opencord.kafka.EventBusService;

import java.util.List;

/**
 * set of unit test cases for McastKafkaIntegration.
 */
public class McastKafkaIntegrationTest {
    private static final String MCAST_OPERATIONAL_STATUS_TOPIC = "mcastOperationalStatus.events";
    private McastKafkaIntegration mcastKafkaIntegration;
    private CordMcastStatisticsEventListener mcastStatsListerner;

    @BeforeEach
    void setup() {
        mcastKafkaIntegration = new McastKafkaIntegration();
        mcastKafkaIntegration.eventBusService = new MockEventBusService();
        mcastKafkaIntegration.cordMcastStatisticsService = new MockCordMcastStatisticsService();
        mcastKafkaIntegration.bindMcastStatisticsService(mcastKafkaIntegration.cordMcastStatisticsService);
        mcastKafkaIntegration.activate();
    }

    @AfterEach
    void tearDown() {
        mcastKafkaIntegration.unbindMcastStatisticsService(mcastKafkaIntegration.cordMcastStatisticsService);
        mcastKafkaIntegration.deactivate();
    }

    /**
     * test to verify CordMcastStatisticsEvent event.
     */
    @Test
    void testHandleEvent() {
        List<CordMcastStatistics> statsList = Lists.newArrayList(
                new CordMcastStatistics(IpAddress.valueOf("172.16.34.34"),
                        "192.168.0.21", VlanId.vlanId("100")),
                new CordMcastStatistics(IpAddress.valueOf("172.16.35.35"),
                        "192.168.0.22", VlanId.vlanId("101"))
        );
        CordMcastStatisticsEvent event = new CordMcastStatisticsEvent(
                CordMcastStatisticsEvent.Type.STATUS_UPDATE, statsList);
        mcastStatsListerner.event(event);
        Assert.assertNotEquals(MockEventBusService.mcastStats, 0);

    }

    private static class MockEventBusService implements EventBusService {
        static int mcastStats = 0;

        @Override
        public void send(String topic, JsonNode data) {
            if (topic.equals(MCAST_OPERATIONAL_STATUS_TOPIC)) {
                mcastStats++;
            }
        }
    }

    private class MockCordMcastStatisticsService implements CordMcastStatisticsService {
        @Override
        public void setVlanValue(VlanId vlanId) {
        }

        @Override
        public void addListener(CordMcastStatisticsEventListener listener) {
            mcastStatsListerner = listener;
        }

        @Override
        public void removeListener(CordMcastStatisticsEventListener listener) {
            mcastStatsListerner = null;
        }
    }
}
