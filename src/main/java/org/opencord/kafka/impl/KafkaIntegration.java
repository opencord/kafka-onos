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
package org.opencord.kafka.impl;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.onosproject.cluster.ClusterService;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.net.config.ConfigFactory;
import org.onosproject.net.config.NetworkConfigEvent;
import org.onosproject.net.config.NetworkConfigListener;
import org.onosproject.net.config.NetworkConfigRegistry;
import org.onosproject.net.config.basics.SubjectFactories;
import org.opencord.kafka.EventBusService;
import org.slf4j.Logger;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.onlab.util.Tools.groupedThreads;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Sends events to a Kafka event bus.
 */
@Service
@Component(immediate = true)
public class KafkaIntegration implements EventBusService {

    private final Logger log = getLogger(getClass());
    private static final Class<KafkaConfig>
            KAFKA_CONFIG_CLASS = KafkaConfig.class;

    private static final String APP_NAME = "org.opencord.kafka";
    private ApplicationId appId;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ClusterService clusterService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected NetworkConfigRegistry configRegistry;

    private static StringSerializer stringSerializer = new StringSerializer();

    private KafkaProducer<String, String> kafkaProducer;

    private InternalNetworkConfigListener configListener =
            new InternalNetworkConfigListener();

    private final ExecutorService executor = newSingleThreadExecutor(
            groupedThreads(this.getClass().getSimpleName(), "events", log));

    private ConfigFactory<ApplicationId, KafkaConfig> kafkaConfigFactory =
            new ConfigFactory<ApplicationId, KafkaConfig>(
                    SubjectFactories.APP_SUBJECT_FACTORY, KAFKA_CONFIG_CLASS,
                    "kafka") {
                @Override
                public KafkaConfig createConfig() {
                    return new KafkaConfig();
                }
            };

    private static final String CLIENT_ID = "client.id";
    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String RETRIES = "retries";
    private static final String RECONNECT_BACKOFF = "reconnect.backoff.ms";
    private static final String INFLIGHT_REQUESTS =
            "max.in.flight.requests.per.connection";
    private static final String ACKS = "acks";
    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String STRING_SERIALIZER =
            stringSerializer.getClass().getCanonicalName();

    private static final String TIMESTAMP = "timestamp";

    @Activate
    public void activate() {
        appId = coreService.registerApplication(APP_NAME);
        configRegistry.registerConfigFactory(kafkaConfigFactory);
        configRegistry.addListener(configListener);
        configure();

        log.info("Started");
    }

    @Deactivate
    public void deactivate() {
        configRegistry.removeListener(configListener);
        configRegistry.unregisterConfigFactory(kafkaConfigFactory);

        executor.shutdownNow();

        shutdownKafka();
        log.info("Stopped");
    }

    private void configure() {
        KafkaConfig config =
                configRegistry.getConfig(appId, KAFKA_CONFIG_CLASS);
        if (config == null) {
            log.info("Kafka configuration not present");
            return;
        }
        configure(config);
    }

    private void configure(KafkaConfig config) {
        checkNotNull(config);

        Properties properties = new Properties();
        properties.put(CLIENT_ID, clusterService.getLocalNode().id().toString());
        properties.put(BOOTSTRAP_SERVERS, config.getBootstrapServers());
        properties.put(RETRIES, config.getRetries());
        properties.put(RECONNECT_BACKOFF, config.getReconnectBackoff());
        properties.put(INFLIGHT_REQUESTS, config.getInflightRequests());
        properties.put(ACKS, config.getAcks());
        properties.put(KEY_SERIALIZER, STRING_SERIALIZER);
        properties.put(VALUE_SERIALIZER, STRING_SERIALIZER);

        startKafka(properties);
    }

    private void unconfigure() {
        shutdownKafka();
    }

    private void startKafka(Properties properties) {
        shutdownKafka();

        // Kafka client doesn't play nice with the default OSGi classloader
        // This workaround temporarily changes the thread's classloader so that
        // the Kafka client can load the serializer classes.
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        try {
            log.info("Starting Kafka producer");
            kafkaProducer = new KafkaProducer<>(properties);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    private void shutdownKafka() {
        if (kafkaProducer != null) {
            log.info("Shutting down Kafka producer");
            kafkaProducer.flush();
            kafkaProducer.close(0, TimeUnit.MILLISECONDS);
            kafkaProducer = null;
        }
    }

    private void logException(Exception e) {
        if (e != null) {
            log.error("Exception while sending to Kafka", e);
        }
    }

    @Override
    public void send(String topic, JsonNode data) {
        if (kafkaProducer == null) {
            log.warn("Not sending event as kafkaProducer is not defined: {}", data.toString());
            return;
        }

        if (log.isTraceEnabled()) {
            log.trace("Sending event to Kafka: {}", data.toString());
        }

        kafkaProducer.send(new ProducerRecord<>(topic, data.toString()),
                (r, e) -> logException(e));
    }

    private class InternalNetworkConfigListener implements NetworkConfigListener {

        @Override
        public void event(NetworkConfigEvent event) {
            log.info("Event type {}", event.type());
            switch (event.type()) {
            case CONFIG_ADDED:
            case CONFIG_UPDATED:
                configure((KafkaConfig) event.config().get());
                break;
            case CONFIG_REMOVED:
                unconfigure();
                break;
            case CONFIG_REGISTERED:
            case CONFIG_UNREGISTERED:
            default:
                break;
            }
        }

        @Override
        public boolean isRelevant(NetworkConfigEvent event) {
            return event.configClass().equals(KAFKA_CONFIG_CLASS);
        }
    }
}
