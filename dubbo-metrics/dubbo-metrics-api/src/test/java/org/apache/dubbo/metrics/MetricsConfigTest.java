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
package org.apache.dubbo.metrics;

import org.apache.dubbo.config.MetricsConfig;
import org.apache.dubbo.config.context.ConfigManager;
import org.apache.dubbo.config.context.ModuleConfigManager;
import org.apache.dubbo.rpc.model.ApplicationModel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.dubbo.common.constants.MetricsConstants.PROTOCOL_PROMETHEUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetricsConfigTest {

    private ConfigManager configManager;
    private ModuleConfigManager moduleConfigManager;

    @BeforeEach
    public void init() {
        ApplicationModel.defaultModel().destroy();
        ApplicationModel applicationModel = ApplicationModel.defaultModel();
        configManager = applicationModel.getApplicationConfigManager();
        moduleConfigManager = applicationModel.getDefaultModule().getConfigManager();
    }

    // Test MetricsConfig correlative methods
    @Test
    void testMetricsConfig() {
        MetricsConfig config = new MetricsConfig();
        config.setProtocol(PROTOCOL_PROMETHEUS);
        configManager.addConfig(config);
        assertTrue(configManager.getMetrics().isPresent());
        assertEquals(config, configManager.getMetrics().get());
        assertEquals(config, moduleConfigManager.getMetrics().get());
    }
}
