/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.config.context;

import org.apache.dubbo.config.AbstractConfig;
import org.apache.dubbo.config.annotation.DubboProperties;

import java.util.concurrent.ConcurrentHashMap;

public class DubboConfigWrapper<T extends AbstractConfig> {

    private final Class<T> configType;

    private final DubboProperties dubboProperties;

    private final ConcurrentHashMap<String, T> configsMap;

    public DubboConfigWrapper(Class<T> configClass) {
        DubboProperties dubboProperties = configClass.getAnnotation(DubboProperties.class);
        if (dubboProperties == null) {
            throw new IllegalArgumentException("Unsupported config type: " + configClass);
        }
        this.configType = configClass;
        this.dubboProperties = dubboProperties;
        this.configsMap = new ConcurrentHashMap<>();
    }

    public DubboConfigWrapper(String key, T config) {
        this((Class<T>) config.getClass());
        this.configsMap.put(key, config);
    }

    public Class<T> getConfigType() {
        return configType;
    }

    public ConcurrentHashMap<String, T> getConfigsMap() {
        return configsMap;
    }

    public DubboProperties getDubboProperties() {
        return dubboProperties;
    }

    @Override
    public String toString() {
        return "DubboConfigWrapper{" + "configType="
                + configType + ", dubboProperties="
                + dubboProperties + ", configsMap="
                + configsMap + '}';
    }
}
