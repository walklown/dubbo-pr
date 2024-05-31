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

import org.apache.dubbo.common.context.ModuleExt;
import org.apache.dubbo.common.extension.DisableInject;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.config.AbstractConfig;
import org.apache.dubbo.config.AbstractInterfaceConfig;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ConfigCenterConfig;
import org.apache.dubbo.config.ConfigScope;
import org.apache.dubbo.config.ConsumerConfig;
import org.apache.dubbo.config.MetadataReportConfig;
import org.apache.dubbo.config.MetricsConfig;
import org.apache.dubbo.config.ModuleConfig;
import org.apache.dubbo.config.MonitorConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.ProviderConfig;
import org.apache.dubbo.config.ReferenceConfigBase;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfigBase;
import org.apache.dubbo.config.SslConfig;
import org.apache.dubbo.config.TracingConfig;
import org.apache.dubbo.config.annotation.DubboProperties;
import org.apache.dubbo.rpc.model.ModuleModel;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.COMMON_UNEXPECTED_EXCEPTION;

/**
 * Manage configs of module
 */
public class ModuleConfigManager extends AbstractConfigManager implements ModuleExt {

    private static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(ModuleConfigManager.class);

    public static final String NAME = "moduleConfig";

    private final Map<String, AbstractInterfaceConfig> serviceConfigCache = new ConcurrentHashMap<>();
    private final ConfigManager applicationConfigManager;

    public ModuleConfigManager(ModuleModel moduleModel) {
        super(moduleModel, ConfigScope.MODULE);
        applicationConfigManager = moduleModel.getApplicationModel().getApplicationConfigManager();
    }

    // ModuleConfig correlative methods

    @DisableInject
    public void setModule(ModuleConfig module) {
        addConfig(module);
    }

    public Optional<ModuleConfig> getModule() {
        return findConfig(ModuleConfig.class);
    }

    // ServiceConfig correlative methods

    public void addService(ServiceConfigBase<?> serviceConfig) {
        addConfig(serviceConfig);
    }

    public void addServices(Iterable<ServiceConfigBase<?>> serviceConfigs) {
        serviceConfigs.forEach(this::addService);
    }

    public Collection<ServiceConfigBase> getServices() {
        return getRepeatableConfigs(ServiceConfigBase.class);
    }

    public <T> ServiceConfigBase<T> getService(String id) {
        return getConfig(ServiceConfigBase.class, id);
    }

    // ReferenceConfig correlative methods

    public void addReference(ReferenceConfigBase<?> referenceConfig) {
        addConfig(referenceConfig);
    }

    public void addReferences(Iterable<ReferenceConfigBase<?>> referenceConfigs) {
        referenceConfigs.forEach(this::addReference);
    }

    public Collection<ReferenceConfigBase<?>> getReferences() {
        Collection<ReferenceConfigBase> configs = getRepeatableConfigs(ReferenceConfigBase.class);
        return configs.stream().map(config -> (ReferenceConfigBase<?>) config).collect(Collectors.toList());
    }

    public <T> ReferenceConfigBase<T> getReference(String id) {
        return getConfig(ReferenceConfigBase.class, id);
    }

    public void addProvider(ProviderConfig providerConfig) {
        addConfig(providerConfig);
    }

    public void addProviders(Iterable<ProviderConfig> providerConfigs) {
        providerConfigs.forEach(this::addProvider);
    }

    public Optional<ProviderConfig> getProvider(String id) {
        return findConfig(ProviderConfig.class, id);
    }

    /**
     * Only allows one default ProviderConfig
     */
    public Optional<ProviderConfig> getDefaultProvider() {
        List<ProviderConfig> providerConfigs = getDefaultConfigs(ProviderConfig.class);
        if (CollectionUtils.isNotEmpty(providerConfigs)) {
            return Optional.of(providerConfigs.get(0));
        }
        return Optional.empty();
    }

    public Collection<ProviderConfig> getProviders() {
        return getRepeatableConfigs(ProviderConfig.class);
    }

    // ConsumerConfig correlative methods

    public void addConsumer(ConsumerConfig consumerConfig) {
        addConfig(consumerConfig);
    }

    public void addConsumers(Iterable<ConsumerConfig> consumerConfigs) {
        consumerConfigs.forEach(this::addConsumer);
    }

    public Optional<ConsumerConfig> getConsumer(String id) {
        return findConfig(ConsumerConfig.class, id);
    }

    /**
     * Only allows one default ConsumerConfig
     */
    public Optional<ConsumerConfig> getDefaultConsumer() {
        List<ConsumerConfig> consumerConfigs = getDefaultConfigs(ConsumerConfig.class);
        if (CollectionUtils.isNotEmpty(consumerConfigs)) {
            return Optional.of(consumerConfigs.get(0));
        }
        return Optional.empty();
    }

    public Collection<ConsumerConfig> getConsumers() {
        return getRepeatableConfigs(ConsumerConfig.class);
    }

    @Override
    public void refreshAll() {
        // refresh all configs here
        getModule().ifPresent(ModuleConfig::refresh);
        getProviders().forEach(ProviderConfig::refresh);
        getConsumers().forEach(ConsumerConfig::refresh);

        getReferences().forEach(ReferenceConfigBase::refresh);
        getServices().forEach(ServiceConfigBase::refresh);
    }

    @Override
    public void clear() {
        super.clear();
        this.serviceConfigCache.clear();
    }

    @Override
    protected <C extends AbstractConfig> Optional<C> findDuplicatedConfig(
            DubboConfigWrapper<C> configWrapper, C config) {
        // check duplicated configs
        // special check service and reference config by unique service name, speed up the processing of large number of
        // instances
        if (config instanceof ReferenceConfigBase || config instanceof ServiceConfigBase) {
            C existedConfig = (C) findDuplicatedInterfaceConfig((AbstractInterfaceConfig) config);
            if (existedConfig != null) {
                return Optional.of(existedConfig);
            }
        } else {
            return super.findDuplicatedConfig(configWrapper, config);
        }
        return Optional.empty();
    }

    @Override
    protected <C extends AbstractConfig> boolean removeIfAbsent(C config, Map<String, C> configsMap) {
        if (super.removeIfAbsent(config, configsMap)) {
            if (config instanceof ReferenceConfigBase || config instanceof ServiceConfigBase) {
                removeInterfaceConfig((AbstractInterfaceConfig) config);
            }
            return true;
        }
        return false;
    }

    /**
     * check duplicated ReferenceConfig/ServiceConfig
     *
     * @param config
     */
    private AbstractInterfaceConfig findDuplicatedInterfaceConfig(AbstractInterfaceConfig config) {
        String uniqueServiceName;
        Map<String, AbstractInterfaceConfig> configCache;
        if (config instanceof ReferenceConfigBase) {
            return null;
        } else if (config instanceof ServiceConfigBase) {
            ServiceConfigBase serviceConfig = (ServiceConfigBase) config;
            uniqueServiceName = serviceConfig.getUniqueServiceName();
            configCache = serviceConfigCache;
        } else {
            throw new IllegalArgumentException(
                    "Illegal type of parameter 'config' : " + config.getClass().getName());
        }

        AbstractInterfaceConfig prevConfig = configCache.putIfAbsent(uniqueServiceName, config);
        if (prevConfig != null) {
            if (prevConfig == config) {
                return prevConfig;
            }

            if (prevConfig.equals(config)) {
                // Is there any problem with ignoring duplicate and equivalent but different ReferenceConfig instances?
                if (logger.isWarnEnabled() && duplicatedConfigs.add(config)) {
                    logger.warn(COMMON_UNEXPECTED_EXCEPTION, "", "", "Ignore duplicated and equal config: " + config);
                }
                return prevConfig;
            }

            String configType = config.getClass().getSimpleName();
            String msg = "Found multiple " + configType + "s with unique service name [" + uniqueServiceName
                    + "], previous: " + prevConfig + ", later: " + config + ". " + "There can only be one instance of "
                    + configType + " with the same triple (group, interface, version). "
                    + "If multiple instances are required for the same interface, please use a different group or version.";

            if (logger.isWarnEnabled() && duplicatedConfigs.add(config)) {
                logger.warn(COMMON_UNEXPECTED_EXCEPTION, "", "", msg);
            }
            if (!this.ignoreDuplicatedInterface) {
                throw new IllegalStateException(msg);
            }
        }
        return prevConfig;
    }

    private void removeInterfaceConfig(AbstractInterfaceConfig config) {
        String uniqueServiceName;
        Map<String, AbstractInterfaceConfig> configCache;
        if (config instanceof ReferenceConfigBase) {
            return;
        } else if (config instanceof ServiceConfigBase) {
            ServiceConfigBase serviceConfig = (ServiceConfigBase) config;
            uniqueServiceName = serviceConfig.getUniqueServiceName();
            configCache = serviceConfigCache;
        } else {
            throw new IllegalArgumentException(
                    "Illegal type of parameter 'config' : " + config.getClass().getName());
        }
        configCache.remove(uniqueServiceName, config);
    }

    @Override
    public void loadConfigs() {
        // load dubbo.providers.xxx
        loadConfigsOfTypeFromProps(ProviderConfig.class);

        // load dubbo.consumers.xxx
        loadConfigsOfTypeFromProps(ConsumerConfig.class);

        // load dubbo.modules.xxx
        loadConfigsOfTypeFromProps(ModuleConfig.class);

        // check configs
        checkDefaultAndValidateConfigs(ProviderConfig.class);
        checkDefaultAndValidateConfigs(ConsumerConfig.class);
        checkDefaultAndValidateConfigs(ModuleConfig.class);
    }

    //
    // Delegate read application configs
    //

    public ConfigManager getApplicationConfigManager() {
        return applicationConfigManager;
    }

    public <C extends AbstractConfig> Map<String, C> getConfigsMap(Class<? extends C> cls) {
        ConfigScope configScope = resolveConfigScope(cls);
        return ConfigScope.APPLICATION.equals(configScope)
                ? applicationConfigManager.getConfigsMap(cls)
                : super.getConfigsMap(cls);
    }

    @Override
    public <T extends AbstractConfig> Optional<T> findConfig(Class<T> cls) {
        ConfigScope configScope = resolveConfigScope(cls);
        return ConfigScope.APPLICATION.equals(configScope)
                ? applicationConfigManager.findConfig(cls)
                : super.findConfig(cls);
    }

    @Override
    public <C extends AbstractConfig> C getConfig(Class<? extends C> cls) {
        ConfigScope configScope = resolveConfigScope(cls);
        return ConfigScope.APPLICATION.equals(configScope)
                ? applicationConfigManager.getConfig(cls)
                : super.getConfig(cls);
    }

    @Override
    public <C extends AbstractConfig> Collection<C> getRepeatableConfigs(Class<? extends C> cls) {
        ConfigScope configScope = resolveConfigScope(cls);
        return ConfigScope.APPLICATION.equals(configScope)
                ? applicationConfigManager.getRepeatableConfigs(cls)
                : super.getRepeatableConfigs(cls);
    }

    @Override
    public <C extends AbstractConfig> Optional<C> findConfig(Class<? extends C> cls, String idOrName) {
        ConfigScope configScope = resolveConfigScope(cls);
        return ConfigScope.APPLICATION.equals(configScope)
                ? applicationConfigManager.findConfig(cls, idOrName)
                : super.findConfig(cls, idOrName);
    }

    @Override
    public <C extends AbstractConfig> C getConfig(Class<? extends C> cls, String idOrName) {
        ConfigScope configScope = resolveConfigScope(cls);
        return ConfigScope.APPLICATION.equals(configScope)
                ? applicationConfigManager.getConfig(cls, idOrName)
                : super.getConfig(cls, idOrName);
    }

    @Override
    public <C extends AbstractConfig> List<C> getDefaultConfigs(Class<C> cls) {
        ConfigScope configScope = resolveConfigScope(cls);
        return ConfigScope.APPLICATION.equals(configScope)
                ? applicationConfigManager.getDefaultConfigs(cls)
                : super.getDefaultConfigs(cls);
    }

    private static ConfigScope resolveConfigScope(Class<?> uniqueConfigClass) {
        DubboProperties dubboProperties = uniqueConfigClass.getAnnotation(DubboProperties.class);
        if (dubboProperties == null) {
            throw new IllegalArgumentException("Unsupported config type: " + uniqueConfigClass);
        }
        return dubboProperties.configScope();
    }

    public Optional<ApplicationConfig> getApplication() {
        return applicationConfigManager.findConfig(ApplicationConfig.class);
    }

    public Optional<MonitorConfig> getMonitor() {
        return applicationConfigManager.findConfig(MonitorConfig.class);
    }

    public Optional<MetricsConfig> getMetrics() {
        return applicationConfigManager.findConfig(MetricsConfig.class);
    }

    public Optional<TracingConfig> getTracing() {
        return applicationConfigManager.findConfig(TracingConfig.class);
    }

    public Optional<SslConfig> getSsl() {
        return applicationConfigManager.findConfig(SslConfig.class);
    }

    public Optional<Collection<ConfigCenterConfig>> getDefaultConfigCenter() {
        return applicationConfigManager.getDefaultConfigCenter();
    }

    public Optional<ConfigCenterConfig> getConfigCenter(String id) {
        return applicationConfigManager.findConfig(ConfigCenterConfig.class, id);
    }

    public Collection<ConfigCenterConfig> getConfigCenters() {
        return applicationConfigManager.getRepeatableConfigs(ConfigCenterConfig.class);
    }

    public Collection<MetadataReportConfig> getMetadataConfigs() {
        return applicationConfigManager.getRepeatableConfigs(MetadataReportConfig.class);
    }

    public Collection<MetadataReportConfig> getDefaultMetadataConfigs() {
        return applicationConfigManager.getDefaultConfigs(MetadataReportConfig.class);
    }

    public Optional<ProtocolConfig> getProtocol(String idOrName) {
        return applicationConfigManager.findConfig(ProtocolConfig.class, idOrName);
    }

    public List<ProtocolConfig> getDefaultProtocols() {
        return applicationConfigManager.getDefaultConfigs(ProtocolConfig.class);
    }

    public Collection<ProtocolConfig> getProtocols() {
        return applicationConfigManager.getRepeatableConfigs(ProtocolConfig.class);
    }

    public Optional<RegistryConfig> getRegistry(String id) {
        return applicationConfigManager.findConfig(RegistryConfig.class, id);
    }

    public List<RegistryConfig> getDefaultRegistries() {
        return applicationConfigManager.getDefaultConfigs(RegistryConfig.class);
    }

    public Collection<RegistryConfig> getRegistries() {
        return applicationConfigManager.getRepeatableConfigs(RegistryConfig.class);
    }
}
