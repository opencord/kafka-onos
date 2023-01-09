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
import org.opencord.igmpproxy.IgmpStatisticType;
import org.opencord.igmpproxy.IgmpStatisticsEventListener;
import org.opencord.igmpproxy.IgmpStatisticsService;
import org.opencord.kafka.EventBusService;

import static org.junit.Assert.assertEquals;

/**
 * set of unit test cases for IgmpKafkaIntegration.
 */
public class IgmpKafkaIntegrationTest extends KafkaIntegrationTestBase {
    private IgmpKafkaIntegration igmpKafkaIntegration;
    private IgmpStatisticsEventListener igmpStatisticsEventListener;

    @BeforeEach
    void setup() {
        igmpKafkaIntegration = new IgmpKafkaIntegration();
        igmpKafkaIntegration.eventBusService = new MockEventBusService();
        igmpKafkaIntegration.ignore = new MockIgmpStatisticsService();
        igmpKafkaIntegration.bindIgmpStatService(igmpKafkaIntegration.ignore);
        igmpKafkaIntegration.activate();
    }

    @AfterEach
    void tearDown() {
        igmpKafkaIntegration.unbindIgmpStatService(igmpKafkaIntegration.ignore);
        igmpKafkaIntegration.deactivate();
    }

    /**
     * test to verify IgmpStatisticsEvent event.
     */
    @Test
    void testIgmpStatisticsEvent() {
        igmpStatisticsEventListener.event(getIgmpStatisticsEvent());
        assertEquals(MockEventBusService.igmpstats, 1);
        assertEquals(MockEventBusService.otherCounter, 0);
    }

    private static class MockEventBusService implements EventBusService {
        static int igmpstats;
        static int otherCounter;

        MockEventBusService() {
            igmpstats = 0;
            otherCounter = 0;
        }

        @Override
        public void send(String topic, JsonNode data) {
            if (topic.equals(IgmpKafkaIntegration.IGMP_STATISTICS_TOPIC)) {
                igmpstats++;
            } else {
                otherCounter++;
            }
        }
    }

    private class MockIgmpStatisticsService implements IgmpStatisticsService {

        @Override
        public void addListener(IgmpStatisticsEventListener listener) {
            igmpStatisticsEventListener = listener;
        }

        @Override
        public void removeListener(IgmpStatisticsEventListener listener) {
            igmpStatisticsEventListener = null;
        }

        @Override
        public void increaseStat(IgmpStatisticType igmpStatisticType) {

        }

        @Override
        public void resetAllStats() {

        }

        @Override
        public Long getStat(IgmpStatisticType igmpStatisticType) {
            return 0L;
        }
    }
}
