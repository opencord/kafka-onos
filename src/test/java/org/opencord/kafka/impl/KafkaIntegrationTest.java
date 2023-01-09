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

package org.opencord.kafka.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.onosproject.cluster.ClusterServiceAdapter;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreServiceAdapter;
import org.onosproject.net.config.Config;
import org.onosproject.net.config.ConfigApplyDelegate;
import org.onosproject.net.config.NetworkConfigEvent;
import org.onosproject.net.config.NetworkConfigListener;
import org.onosproject.net.config.NetworkConfigRegistryAdapter;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * set of unit test to verify KafkaIntegration.
 */
class KafkaIntegrationTest {
    private static final String ASSERT_MESSAGE = "Config not updated";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    protected NetworkConfigListener configListener;
    protected ConfigApplyDelegate delegate;
    protected ObjectMapper mapper;
    protected ApplicationId subject;
    protected NetworkConfigEvent event;
    private KafkaConfig config = new KafkaConfig();
    private KafkaIntegration kafkaIntegration;

    @BeforeEach
    public void setUp() throws Exception {
        kafkaIntegration = new KafkaIntegration();
        kafkaIntegration.coreService = new MockCoreService();
        kafkaIntegration.clusterService = new ClusterServiceAdapter();
        setupConfig("/localKafkaConfig.json");
        kafkaIntegration.activate();
    }

    @AfterEach
    public void tearDown() {
        kafkaIntegration.deactivate();
        kafkaIntegration = null;
    }

    /**
     * Local configuration verification using Resources file.
     */
    @Test
    void testConfigAdded() {
        event = new NetworkConfigEvent(NetworkConfigEvent.Type.CONFIG_ADDED, subject,
                config, null, KafkaConfig.class);
        configListener.event(event);
        KafkaConfig expectedConfig = kafkaIntegration.configRegistry
                .getConfig(kafkaIntegration.coreService.getAppId(KafkaIntegration.APP_NAME),
                        KafkaConfig.class);
        assertEquals(ASSERT_MESSAGE, BOOTSTRAP_SERVERS, expectedConfig.getBootstrapServers());
    }

    public void setupConfig(String localConfig) throws Exception {
        delegate = new MockConfigDelegate();
        mapper = new ObjectMapper();
        subject = kafkaIntegration.coreService.registerApplication(KafkaIntegration.APP_NAME);
        config.init(subject, "kafka-local-mode-test", node(localConfig), mapper, delegate);
        kafkaIntegration.configRegistry = new MockNetworkConfigRegistry(subject, config);
    }

    protected JsonNode node(String jsonFile) throws Exception {
        final InputStream jsonStream = KafkaConfig.class.getResourceAsStream(jsonFile);
        return mapper.readTree(jsonStream);
    }

    /**
     * Mocks an ONOS configuration delegate to allow JSON based configuration to
     * be tested.
     */
    private static final class MockConfigDelegate implements ConfigApplyDelegate {
        @Override
        public void onApply(@SuppressWarnings({"rawtypes", "TypeParameterUnusedInFormals"}) Config config) {
            config.apply();
        }
    }

    /**
     * Mocks Core service adapter.
     */
    private static class MockCoreService extends CoreServiceAdapter {

        private List<ApplicationId> idList = new ArrayList<>();
        private Map<String, ApplicationId> idMap = new HashMap<>();

        /*
         * (non-Javadoc)
         *
         * @see
         * org.onosproject.core.CoreServiceAdapter#getAppId(java.lang.Short)
         */
        @Override
        public ApplicationId getAppId(Short id) {
            if (id >= idList.size()) {
                return null;
            }
            return idList.get(id);
        }

        /*
         * (non-Javadoc)
         *
         * @see
         * org.onosproject.core.CoreServiceAdapter#getAppId(java.lang.String)
         */
        @Override
        public ApplicationId getAppId(String name) {
            return idMap.get(name);
        }

        /*
         * (non-Javadoc)
         *
         * @see
         * org.onosproject.core.CoreServiceAdapter#registerApplication(java.lang
         * .String)
         */
        @Override
        public ApplicationId registerApplication(String name) {
            ApplicationId appId = idMap.get(name);
            if (appId == null) {
                appId = new MockApplicationId((short) idList.size(), name);
                idList.add(appId);
                idMap.put(name, appId);
            }
            return appId;
        }
    }

    /*
   Mocks application id.
    */
    private static class MockApplicationId implements ApplicationId {

        private final short id;
        private final String name;

        public MockApplicationId(short id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public short id() {
            return id;
        }

        @Override
        public String name() {
            return name;
        }
    }


    /**
     * Mocks the ONOS network configuration registry so that the application
     * under test can access a JSON defined configuration.
     */
    private class MockNetworkConfigRegistry<S> extends NetworkConfigRegistryAdapter {
        private final KafkaConfig config;

        public MockNetworkConfigRegistry(final S subject, final KafkaConfig config) {
            this.config = config;
        }

        @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
        @Override
        public <S, C extends Config<S>> C getConfig(final S subject, final Class<C> configClass) {
            return (C) config;
        }

        @Override
        public void addListener(NetworkConfigListener listener) {
            configListener = listener;
        }
    }
}