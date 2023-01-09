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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.onlab.packet.ChassisId;
import org.onosproject.net.AnnotationKeys;
import org.onosproject.net.Annotations;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.DefaultAnnotations;
import org.onosproject.net.DefaultDevice;
import org.onosproject.net.Device;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Element;
import org.onosproject.net.ElementId;
import org.onosproject.net.Port;
import org.onosproject.net.PortNumber;
import org.onosproject.net.device.DeviceServiceAdapter;
import org.onosproject.net.device.PortStatistics;
import org.onosproject.net.driver.Behaviour;
import org.onosproject.net.provider.ProviderId;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * DeviceServiceAdapter mocker class.
 */
public class MockDeviceService extends DeviceServiceAdapter {

    public static final String OLT_DEV_ID = "of:0000c6b1cd40dc93";
    public static final DeviceId DEVICE_ID_1 = DeviceId.deviceId(OLT_DEV_ID);
    private static final String SCHEME_NAME = "kafka-onos";
    private static final DefaultAnnotations DEVICE_ANNOTATIONS = DefaultAnnotations.builder()
            .set(AnnotationKeys.PROTOCOL, SCHEME_NAME.toUpperCase()).build();
    private final ProviderId providerId = new ProviderId("of", "foo");
    private final Device device1 = new MockDevice(providerId, DEVICE_ID_1, Device.Type.SWITCH,
            "foo.inc", "0", "0", OLT_DEV_ID, new ChassisId(),
            DEVICE_ANNOTATIONS);


    @Override
    public Device getDevice(DeviceId devId) {
        return device1;
    }

    @Override
    public Iterable<Device> getDevices() {
        List<Device> devices = new ArrayList<>();
        devices.add(device1);
        return devices;
    }

    @Override
    public Port getPort(ConnectPoint cp) {
        return new MockPort();
    }

    @Override
    public List<PortStatistics> getPortStatistics(DeviceId deviceId) {
        PortStatistics ps = new PortStatistics() {
            @Override
            public PortNumber portNumber() {
                return PortNumber.portNumber(1);
            }

            @Override
            public long packetsReceived() {
                return 100;
            }

            @Override
            public long packetsSent() {
                return 10;
            }

            @Override
            public long bytesReceived() {
                return 100;
            }

            @Override
            public long bytesSent() {
                return 10;
            }

            @Override
            public long packetsRxDropped() {
                return 0;
            }

            @Override
            public long packetsTxDropped() {
                return 1;
            }

            @Override
            public long packetsRxErrors() {
                return 1;
            }

            @Override
            public long packetsTxErrors() {
                return 1;
            }

            @Override
            public long durationSec() {
                return 100;
            }

            @Override
            public long durationNano() {
                return 100 * 1000;
            }

            @Override
            public boolean isZero() {
                return false;
            }
        };
        return Lists.newArrayList(ps);
    }

    /**
     * Port object mock.
     */
    public static class MockPort implements Port {

        @Override
        public boolean isEnabled() {
            return true;
        }

        public long portSpeed() {
            return 1000;
        }

        public Element element() {
            return new Element() {
                @Override
                public ElementId id() {
                    return DEVICE_ID_1;
                }

                @Override
                public Annotations annotations() {
                    return null;
                }

                @Override
                public ProviderId providerId() {
                    return null;
                }

                @Override
                @SuppressWarnings({"TypeParameterUnusedInFormals"})
                public <B extends Behaviour> B as(Class<B> projectionClass) {
                    return null;
                }

                @Override
                public <B extends Behaviour> boolean is(Class<B> projectionClass) {
                    return false;
                }
            };
        }

        public PortNumber number() {
            return PortNumber.portNumber(1);
        }

        public Annotations annotations() {
            return new MockAnnotations();
        }

        public Type type() {
            return Port.Type.FIBER;
        }

        private static class MockAnnotations implements Annotations {

            @Override
            public String value(String val) {
                return "nni-";
            }

            public Set<String> keys() {
                return Sets.newHashSet("portName");
            }
        }
    }

    /**
     * Device mock.
     */
    protected static class MockDevice extends DefaultDevice {

        /*
        Mocks OLT device.
         */
        public MockDevice(ProviderId providerId, DeviceId id, Type type,
                          String manufacturer, String hwVersion, String swVersion,
                          String serialNumber, ChassisId chassisId, Annotations... annotations) {
            super(providerId, id, type, manufacturer, hwVersion, swVersion, serialNumber,
                    chassisId, annotations);
        }
    }
}
