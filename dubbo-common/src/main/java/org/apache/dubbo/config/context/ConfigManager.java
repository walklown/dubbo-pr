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

import org.apache.dubbo.common.context.ApplicationExt;
import org.apache.dubbo.common.extension.DisableInject;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.AbstractConfig;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ConfigCenterConfig;
import org.apache.dubbo.config.ConfigKeys;
import org.apache.dubbo.config.ConfigScope;
import org.apache.dubbo.config.DubboApplicationConfig;
import org.apache.dubbo.config.MetadataReportConfig;
import org.apache.dubbo.config.MetricsConfig;
import org.apache.dubbo.config.MonitorConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.SslConfig;
import org.apache.dubbo.config.TracingConfig;
import org.apache.dubbo.config.annotation.DubboProperties;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

/**
 * A lock-free config manager (through ConcurrentHashMap), for fast read operation.
 * The Write operation lock with sub configs map of config type, for safely check and add new config.
 */
public class ConfigManager extends AbstractConfigManager implements ApplicationExt {

    private static final Logger logger = LoggerFactory.getLogger(ConfigManager.class);

    public static final String NAME = "config";
    public static final String BEAN_NAME = "dubboConfigManager";
    public static final String DUBBO_CONFIG_MODE = ConfigKeys.DUBBO_CONFIG_MODE;

    public ConfigManager(ApplicationModel applicationModel) {
        super(applicationModel, ConfigScope.APPLICATION);
    }

    // ApplicationConfig correlative methods

    /**
     * Set application config
     *
     * @param application
     * @return current application config instance
     * @deprecated {@link ConfigManager#addConfig(AbstractConfig)}
     */
    @Deprecated
    @DisableInject
    public void setApplication(ApplicationConfig application) {
        addConfig(application);
    }

    /**
     * @deprecated {@link ConfigManager#getConfig(Class)}
     */
    @Deprecated
    public Optional<ApplicationConfig> getApplication() {
        return findConfig(ApplicationConfig.class);
    }

    public ApplicationConfig getApplicationOrElseThrow() {
        Optional<ApplicationConfig> optional = ofNullable(getConfig(ApplicationConfig.class));
        return optional.orElseThrow(() -> new IllegalStateException("There's no ApplicationConfig specified."));
    }

    // MonitorConfig correlative methods

    /**
     * @deprecated {@link ConfigManager#addConfig(AbstractConfig)}
     */
    @Deprecated
    @DisableInject
    public void setMonitor(MonitorConfig monitor) {
        addConfig(monitor);
    }

    /**
     * @deprecated {@link ConfigManager#getConfig(Class)}
     */
    @Deprecated
    public Optional<MonitorConfig> getMonitor() {
        return findConfig(MonitorConfig.class);
    }

    /**
     * @deprecated {@link ConfigManager#addConfig(AbstractConfig)}
     */
    @Deprecated
    @DisableInject
    public void setMetrics(MetricsConfig metrics) {
        addConfig(metrics);
    }

    /**
     * @deprecated {@link ConfigManager#getConfig(Class)}
     */
    @Deprecated
    public Optional<MetricsConfig> getMetrics() {
        return findConfig(MetricsConfig.class);
    }

    /**
     * @deprecated {@link ConfigManager#addConfig(AbstractConfig)}
     */
    @Deprecated
    @DisableInject
    public void setTracing(TracingConfig tracing) {
        addConfig(tracing);
    }

    /**
     * @deprecated {@link ConfigManager#getConfig(Class)}
     */
    @Deprecated
    public Optional<TracingConfig> getTracing() {
        return findConfig(TracingConfig.class);
    }

    /**
     * @deprecated {@link ConfigManager#addConfig(AbstractConfig)}
     */
    @Deprecated
    @DisableInject
    public void setSsl(SslConfig sslConfig) {
        addConfig(sslConfig);
    }

    /**
     * @deprecated {@link ConfigManager#getConfig(Class)}
     */
    @Deprecated
    public Optional<SslConfig> getSsl() {
        return findConfig(SslConfig.class);
    }

    // ConfigCenterConfig correlative methods

    /**
     * @deprecated {@link ConfigManager#addConfig(AbstractConfig)}
     */
    @Deprecated
    public void addConfigCenter(ConfigCenterConfig configCenter) {
        addConfig(configCenter);
    }

    /**
     * @deprecated {@link ConfigManager#addConfigs(Iterable)}
     */
    @Deprecated
    public void addConfigCenters(Iterable<ConfigCenterConfig> configCenters) {
        addConfigs(configCenters);
    }

    /**
     * @deprecated {@link ConfigManager#getDefaultConfigs(Class)}
     */
    @Deprecated
    public Optional<Collection<ConfigCenterConfig>> getDefaultConfigCenter() {
        Collection<ConfigCenterConfig> defaults = getDefaultConfigs(ConfigCenterConfig.class);
        if (CollectionUtils.isEmpty(defaults)) {
            defaults = getRepeatableConfigs(ConfigCenterConfig.class);
        }
        return Optional.ofNullable(defaults);
    }

    /**
     * @deprecated {@link ConfigManager#getConfig(Class, String)}
     */
    @Deprecated
    public Optional<ConfigCenterConfig> getConfigCenter(String id) {
        return findConfig(ConfigCenterConfig.class, id);
    }

    /**
     * @deprecated {@link ConfigManager#getRepeatableConfigs(Class)}
     */
    @Deprecated
    public Collection<ConfigCenterConfig> getConfigCenters() {
        return getRepeatableConfigs(ConfigCenterConfig.class);
    }

    // MetadataReportConfig correlative methods

    /**
     * @deprecated {@link ConfigManager#addConfig(AbstractConfig)}
     */
    @Deprecated
    public void addMetadataReport(MetadataReportConfig metadataReportConfig) {
        addConfig(metadataReportConfig);
    }

    /**
     * @deprecated {@link ConfigManager#addConfigs(Iterable)}
     */
    @Deprecated
    public void addMetadataReports(Iterable<MetadataReportConfig> metadataReportConfigs) {
        metadataReportConfigs.forEach(this::addConfig);
    }

    /**
     * @deprecated {@link ConfigManager#getRepeatableConfigs(Class)}
     */
    @Deprecated
    public Collection<MetadataReportConfig> getMetadataConfigs() {
        return getRepeatableConfigs(MetadataReportConfig.class);
    }

    /**
     * @deprecated {@link ConfigManager#getDefaultConfigs(Class)}
     */
    @Deprecated
    public Collection<MetadataReportConfig> getDefaultMetadataConfigs() {
        Collection<MetadataReportConfig> defaults = getDefaultConfigs(MetadataReportConfig.class);
        if (CollectionUtils.isEmpty(defaults)) {
            return getRepeatableConfigs(MetadataReportConfig.class);
        }
        return defaults;
    }

    // ProtocolConfig correlative methods

    /**
     * @deprecated {@link ConfigManager#addConfig(AbstractConfig)}
     */
    @Deprecated
    public void addProtocol(ProtocolConfig protocolConfig) {
        addConfig(protocolConfig);
    }

    /**
     * @deprecated {@link ConfigManager#addConfigs(Iterable)}
     */
    @Deprecated
    public void addProtocols(Iterable<ProtocolConfig> protocolConfigs) {
        if (protocolConfigs != null) {
            protocolConfigs.forEach(this::addConfig);
        }
    }

    /**
     * @deprecated {@link ConfigManager#getConfig(Class, String)}
     */
    @Deprecated
    public Optional<ProtocolConfig> getProtocol(String idOrName) {
        return findConfig(ProtocolConfig.class, idOrName);
    }

    public ProtocolConfig getOrAddProtocol(String idOrName) {
        Optional<ProtocolConfig> protocol = findConfig(ProtocolConfig.class, idOrName);
        if (protocol.isPresent()) {
            return protocol.get();
        }
        ProtocolConfig protocolConfig = new ProtocolConfig(idOrName);
        addConfig(protocolConfig);
        // addProtocol triggers refresh when other protocols exist in the ConfigManager.
        // So refresh is only done when ProtocolConfig is not refreshed.
        if (!protocolConfig.isRefreshed()) {
            protocolConfig.refresh();
        }
        return protocolConfig;
    }

    /**
     * @deprecated {@link ConfigManager#getDefaultConfigs(Class)}
     */
    @Deprecated
    public List<ProtocolConfig> getDefaultProtocols() {
        return getDefaultConfigs(ProtocolConfig.class);
    }

    /**
     * @deprecated {@link ConfigManager#getRepeatableConfigs(Class)}
     */
    @Deprecated
    public Collection<ProtocolConfig> getProtocols() {
        return getRepeatableConfigs(ProtocolConfig.class);
    }

    // RegistryConfig correlative methods

    /**
     * @deprecated {@link ConfigManager#addConfig(AbstractConfig)}
     */
    @Deprecated
    public void addRegistry(RegistryConfig registryConfig) {
        addConfig(registryConfig);
    }

    /**
     * @deprecated {@link ConfigManager#addConfigs(Iterable)}
     */
    @Deprecated
    public void addRegistries(Iterable<RegistryConfig> registryConfigs) {
        if (registryConfigs != null) {
            registryConfigs.forEach(this::addConfig);
        }
    }

    /**
     * @deprecated {@link ConfigManager#getConfig(Class, String)}
     */
    @Deprecated
    public Optional<RegistryConfig> getRegistry(String id) {
        return findConfig(RegistryConfig.class, id);
    }

    /**
     * @deprecated {@link ConfigManager#getDefaultConfigs(Class)}
     */
    @Deprecated
    public List<RegistryConfig> getDefaultRegistries() {
        return getDefaultConfigs(RegistryConfig.class);
    }

    /**
     * @deprecated {@link ConfigManager#getRepeatableConfigs(Class)}
     */
    @Deprecated
    public Collection<RegistryConfig> getRegistries() {
        return getRepeatableConfigs(RegistryConfig.class);
    }

    @Override
    public void loadConfigs() {
        // application config has load before starting config center
        Map<String, Class<?>> dubboConfigClasses = applicationModel
                .getExtensionLoader(DubboApplicationConfig.class)
                .getExtensionClasses();
        List<Class<? extends AbstractConfig>> dubboApplicationConfigClasses = new LinkedList<>();
        for (Class<?> configType : dubboConfigClasses.values()) {
            if (ConfigCenterConfig.class.equals(configType)) {
                // config centers has bean loaded before starting config center
                continue;
            }
            DubboProperties dubboProperties = configType.getAnnotation(DubboProperties.class);
            if (dubboProperties == null) {
                throw new IllegalArgumentException("Miss DubboProperties on config type: " + configType);
            }
            if (!ConfigScope.APPLICATION.equals(dubboProperties.configScope())) {
                continue;
            }
            dubboApplicationConfigClasses.add((Class<? extends AbstractConfig>) configType);
            loadConfigsOfTypeFromProps((Class<? extends AbstractConfig>) configType);
        }

        refreshAll();

        checkConfigs(dubboApplicationConfigClasses);

        // set model name
        if (StringUtils.isBlank(applicationModel.getModelName())) {
            applicationModel.setModelName(applicationModel.getApplicationName());
        }
    }

    private void checkConfigs(List<Class<? extends AbstractConfig>> dubboApplicationConfigClasses) {
        // check config types (ignore metadata-center)
        List<Class<? extends AbstractConfig>> multipleConfigTypes = dubboApplicationConfigClasses.stream()
                .filter(configType -> !MetadataReportConfig.class.equals(configType))
                .collect(Collectors.toList());

        for (Class<? extends AbstractConfig> configType : multipleConfigTypes) {
            checkDefaultAndValidateConfigs(configType);
        }

        // check port conflicts
        Map<Integer, ProtocolConfig> protocolPortMap = new LinkedHashMap<>();
        for (ProtocolConfig protocol : this.getRepeatableConfigs(ProtocolConfig.class)) {
            Integer port = protocol.getPort();
            if (port == null || port == -1) {
                continue;
            }
            ProtocolConfig prevProtocol = protocolPortMap.get(port);
            if (prevProtocol != null) {
                throw new IllegalStateException("Duplicated port used by protocol configs, port: " + port
                        + ", configs: " + Arrays.asList(prevProtocol, protocol));
            }
            protocolPortMap.put(port, protocol);
        }

        // Log the current configurations.
        logger.info("The current configurations or effective configurations are as follows:");
        for (Class<? extends AbstractConfig> configType : multipleConfigTypes) {
            getRepeatableConfigs(configType).forEach((config) -> logger.info(config.toString()));
        }
    }

    public ConfigMode getConfigMode() {
        return configMode;
    }
}
