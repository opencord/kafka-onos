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

import org.onosproject.event.EventListener;
import org.onosproject.event.ListenerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Abstract implementation of a service-specific Kafka integration which provide
 * convenience methods to dynamically bind/unbind event listener services.
 */
class AbstractKafkaIntegration {

    Logger log = LoggerFactory.getLogger(getClass());

    // OSGi demands dynamic @Reference to use volatile fields. We use a second
    // field to store the actual service implementation reference and use that
    // in the bind/unbind methods. We make sure to add listeners only if one was
    // not already added.

    <S extends ListenerService<?, L>, L extends EventListener<?>> void bindAndAddListener(
            S incomingService, AtomicReference<S> serviceRef, L listener) {
        if (incomingService == null) {
            return;
        }
        if (serviceRef.compareAndSet(null, incomingService)) {
            log.info("Adding listener on {}", incomingService.getClass().getSimpleName());
            incomingService.addListener(listener);
        } else {
            log.warn("Trying to bind AccessDeviceService but it is already bound");
        }
    }

    <S extends ListenerService<?, L>, L extends EventListener<?>> void unbindAndRemoveListener(
            S outgoingService, AtomicReference<S> serviceRef, L listener) {
        if (outgoingService != null &&
                serviceRef.compareAndSet(outgoingService, null)) {
            log.info("Removing listener on {}", outgoingService.getClass().getSimpleName());
            outgoingService.removeListener(listener);
        }
        // Else, ignore. This is not the instance currently bound, or the
        // outgoing service is null.
    }
}
