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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.onlab.packet.VlanId;
import org.opencord.cordmcast.CordMcastStatisticsEventListener;
import org.opencord.cordmcast.CordMcastStatisticsService;
import org.opencord.kafka.EventBusService;

import static org.junit.Assert.assertEquals;

/**
 * set of unit test cases for McastKafkaIntegration.
 */
public class McastKafkaIntegrationTest extends KafkaIntegrationTestBase {
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
        mcastStatsListerner.event(getCordMcastStatisticsEvent());
        assertEquals(MockEventBusService.mcastStats, 1);
        assertEquals(MockEventBusService.otherCounter, 0);

    }

    private static class MockEventBusService implements EventBusService {
        static int mcastStats;
        static int otherCounter;

        MockEventBusService() {
            mcastStats = 0;
            otherCounter = 0;
        }

        @Override
        public void send(String topic, JsonNode data) {
            if (topic.equals(McastKafkaIntegration.MCAST_OPERATIONAL_STATUS_TOPIC)) {
                mcastStats++;
            } else {
                otherCounter++;
            }
        }
    }

    private class MockCordMcastStatisticsService implements CordMcastStatisticsService {
        @Override
        public void setVlanValue(VlanId vlanId) {
        }

        @Override
        public void setInnerVlanValue(VlanId vlanId) {

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
