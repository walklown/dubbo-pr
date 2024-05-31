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

import org.apache.dubbo.common.config.CompositeConfiguration;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.config.Environment;
import org.apache.dubbo.common.config.PropertiesConfiguration;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.context.LifecycleAdapter;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.AbstractConfig;
import org.apache.dubbo.config.ConfigKeys;
import org.apache.dubbo.config.ConfigScope;
import org.apache.dubbo.config.DubboApplicationConfig;
import org.apache.dubbo.config.MetadataReportConfig;
import org.apache.dubbo.config.ReferenceConfigBase;
import org.apache.dubbo.config.ServiceConfigBase;
import org.apache.dubbo.config.annotation.DubboProperties;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ScopeModel;
import org.apache.dubbo.rpc.model.ScopeModelUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyMap;
import static java.util.Optional.ofNullable;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.COMMON_PROPERTY_TYPE_MISMATCH;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.COMMON_UNEXPECTED_EXCEPTION;
import static org.apache.dubbo.config.AbstractConfig.getTagName;

public abstract class AbstractConfigManager extends LifecycleAdapter {

    private static final String CONFIG_NAME_READ_METHOD = "getName";

    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(AbstractConfigManager.class);

    protected final Map<String, DubboConfigWrapper<?>> configsCache = new ConcurrentHashMap<>();

    private final Map<String, AtomicInteger> configIdIndexes = new ConcurrentHashMap<>();

    protected Set<AbstractConfig> duplicatedConfigs = new ConcurrentHashSet<>();

    protected final ScopeModel scopeModel;
    protected final ApplicationModel applicationModel;
    private final ConfigScope scope;
    private final Environment environment;
    private ConfigValidator configValidator;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    protected ConfigMode configMode = ConfigMode.STRICT;
    protected boolean ignoreDuplicatedInterface = false;

    public AbstractConfigManager(ScopeModel scopeModel, ConfigScope scope) {
        this.scopeModel = scopeModel;
        this.applicationModel = ScopeModelUtil.getApplicationModel(scopeModel);
        this.scope = scope;
        environment = scopeModel.modelEnvironment();
    }

    @Override
    public void initialize() throws IllegalStateException {
        if (!initialized.compareAndSet(false, true)) {
            return;
        }
        CompositeConfiguration configuration = scopeModel.modelEnvironment().getConfiguration();

        // dubbo.config.mode
        String configModeStr = (String) configuration.getProperty(ConfigKeys.DUBBO_CONFIG_MODE);
        try {
            if (StringUtils.hasText(configModeStr)) {
                this.configMode = ConfigMode.valueOf(configModeStr.toUpperCase());
            }
        } catch (Exception e) {
            String msg = "Illegal '" + ConfigKeys.DUBBO_CONFIG_MODE + "' config value [" + configModeStr
                    + "], available values " + Arrays.toString(ConfigMode.values());
            logger.error(COMMON_PROPERTY_TYPE_MISMATCH, "", "", msg, e);
            throw new IllegalArgumentException(msg, e);
        }

        // dubbo.config.ignore-duplicated-interface
        String ignoreDuplicatedInterfaceStr =
                (String) configuration.getProperty(ConfigKeys.DUBBO_CONFIG_IGNORE_DUPLICATED_INTERFACE);
        if (ignoreDuplicatedInterfaceStr != null) {
            this.ignoreDuplicatedInterface = Boolean.parseBoolean(ignoreDuplicatedInterfaceStr);
        }

        // print
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(ConfigKeys.DUBBO_CONFIG_MODE, configMode);
        map.put(ConfigKeys.DUBBO_CONFIG_IGNORE_DUPLICATED_INTERFACE, this.ignoreDuplicatedInterface);
        logger.info("Config settings: " + map);
    }

    /**
     * Add the dubbo {@link AbstractConfig config}
     *
     * @param configs the dubbo {@link AbstractConfig config}
     */
    public final <T extends AbstractConfig> void addConfigs(Iterable<T> configs) {
        configs.forEach(this::addConfig);
        for (T config : configs) {
            addConfig(config);
        }
    }

    /**
     * Add the dubbo {@link AbstractConfig config}
     *
     * @param config the dubbo {@link AbstractConfig config}
     */
    public final <T extends AbstractConfig> T addConfig(T config) {
        if (config == null) {
            return null;
        }
        DubboConfigWrapper<T> configWrapper = (DubboConfigWrapper<T>) configsCache.computeIfAbsent(
                getTagName(config.getClass()), type -> new DubboConfigWrapper<>(config.getClass()));
        // ignore MethodConfig
        if (!scope.equals(configWrapper.getDubboProperties().configScope())) {
            throw new IllegalArgumentException("Unsupported config type: " + config);
        }

        if (config.getScopeModel() != scopeModel) {
            config.setScopeModel(scopeModel);
        }

        Map<String, T> configsMap = configWrapper.getConfigsMap();
        // fast check duplicated equivalent config before write lock
        if (!(config instanceof ReferenceConfigBase || config instanceof ServiceConfigBase)) {
            for (AbstractConfig value : configsMap.values()) {
                if (value.equals(config)) {
                    return (T) value;
                }
            }
        }

        // lock by config type
        synchronized (configWrapper) {
            return (T) addIfAbsent(config, configWrapper);
        }
    }

    /**
     * Add config
     *
     * @param config
     * @param configWrapper
     * @return the existing equivalent config or the new adding config
     * @throws IllegalStateException
     */
    private <C extends AbstractConfig> C addIfAbsent(C config, DubboConfigWrapper<C> configWrapper)
            throws IllegalStateException {
        Map<String, C> configsMap = configWrapper.getConfigsMap();
        if (config == null || configsMap == null) {
            return config;
        }

        // find by value
        Optional<C> prevConfig = findDuplicatedConfig(configWrapper, config);
        if (prevConfig.isPresent()) {
            return prevConfig.get();
        }

        String key = config.getId();
        if (key == null) {
            do {
                // generate key if id is not set
                key = generateConfigId(config);
            } while (configsMap.containsKey(key));
        }

        C existedConfig = configsMap.get(key);
        if (existedConfig != null && !isEquals(existedConfig, config)) {
            String type = config.getClass().getSimpleName();
            logger.warn(
                    COMMON_UNEXPECTED_EXCEPTION,
                    "",
                    "",
                    String.format(
                            "Duplicate %s found, there already has one default %s or more than two %ss have the same id, "
                                    + "you can try to give each %s a different id, override previous config with later config. id: %s, prev: %s, later: %s",
                            type, type, type, type, key, existedConfig, config));
        }

        // override existed config if any
        configsMap.put(key, config);
        return config;
    }

    protected <C extends AbstractConfig> boolean removeIfAbsent(C config, Map<String, C> configsMap) {
        if (config.getId() != null) {
            return configsMap.remove(config.getId(), config);
        }
        return configsMap.values().removeIf(c -> config == c);
    }

    protected <C extends AbstractConfig> Optional<C> findDuplicatedConfig(
            DubboConfigWrapper<C> configWrapper, C config) {
        Map<String, C> configsMap = configWrapper.getConfigsMap();
        // find by value
        Optional<C> prevConfig = findConfigByValue(configsMap.values(), config);
        if (prevConfig.isPresent()) {
            if (prevConfig.get() == config) {
                // the new one is same as existing one
                return prevConfig;
            }

            // ignore duplicated equivalent config
            if (logger.isInfoEnabled() && duplicatedConfigs.add(config)) {
                logger.info("Ignore duplicated config: " + config);
            }
            return prevConfig;
        }

        // check unique config
        return checkUniqueConfig(configWrapper, config);
    }

    public <C extends AbstractConfig> Map<String, C> getConfigsMap(Class<? extends C> cls) {
        String configType = getTagName(cls);
        DubboConfigWrapper<?> dubboConfigWrapper = configsCache.get(configType);
        if (dubboConfigWrapper == null) {
            return emptyMap();
        }
        return (Map<String, C>) dubboConfigWrapper.getConfigsMap();
    }

    public <T extends AbstractConfig> Optional<T> findConfig(Class<T> cls) {
        T config = getConfig(cls);
        if (config == null) {
            return Optional.empty();
        }
        return Optional.of(config);
    }

    public <C extends AbstractConfig> C getConfig(Class<? extends C> cls) {
        Map<String, C> configsMap = getConfigsMap(cls);
        if (configsMap.isEmpty()) {
            return null;
        }
        return configsMap.values().iterator().next();
    }

    public <C extends AbstractConfig> Collection<C> getRepeatableConfigs(Class<? extends C> cls) {
        Map<String, C> configsMap = getConfigsMap(cls);
        return configsMap.values();
    }

    public <C extends AbstractConfig> Optional<C> findConfig(Class<? extends C> cls, String idOrName) {
        C config = getConfig(cls, idOrName);
        return ofNullable(config);
    }

    /**
     * Get config instance by id or by name
     *
     * @param cls      Config type
     * @param idOrName the id or name of the config
     * @return
     */
    public <C extends AbstractConfig> C getConfig(Class<? extends C> cls, String idOrName) {
        Map<String, C> configsMap = getConfigsMap(cls);
        C config = configsMap.get(idOrName);
        if (config == null) {
            config = getConfigByName(cls, configsMap, idOrName);
        }
        return config;
    }
    /**
     * Get config by name if existed
     *
     * @param cls
     * @param name
     * @return
     */
    private <C extends AbstractConfig> C getConfigByName(
            Class<? extends C> cls, Map<String, ? extends C> configsMap, String name) {
        // try to find config by name
        if (ReflectUtils.hasMethod(cls, CONFIG_NAME_READ_METHOD)) {
            List<C> list = configsMap.values().stream()
                    .filter(cfg -> name.equals(getConfigName(cfg)))
                    .collect(Collectors.toList());
            if (list.size() > 1) {
                throw new IllegalStateException("Found more than one config by name: " + name + ", instances: " + list
                        + ". Please remove redundant configs or get config by id.");
            } else if (list.size() == 1) {
                return list.get(0);
            }
        }
        return null;
    }

    private <C extends AbstractConfig> String getConfigName(C config) {
        try {
            return ReflectUtils.getProperty(config, CONFIG_NAME_READ_METHOD);
        } catch (Exception e) {
            return null;
        }
    }

    private <C extends AbstractConfig> Optional<C> findConfigByValue(Collection<C> values, C config) {
        // 1. find same config instance (speed up raw api usage)
        Optional<C> prevConfig = values.stream().filter(val -> val == config).findFirst();
        if (prevConfig.isPresent()) {
            return prevConfig;
        }

        // 2. find equal config
        prevConfig = values.stream().filter(val -> isEquals(val, config)).findFirst();
        return prevConfig;
    }

    private boolean isEquals(AbstractConfig oldOne, AbstractConfig newOne) {
        if (oldOne == newOne) {
            return true;
        }
        if (oldOne == null || newOne == null) {
            return false;
        }
        if (oldOne.getClass() != newOne.getClass()) {
            return false;
        }
        // make both are refreshed or none is refreshed
        if (oldOne.isRefreshed() || newOne.isRefreshed()) {
            if (!oldOne.isRefreshed()) {
                oldOne.refresh();
            }
            if (!newOne.isRefreshed()) {
                newOne.refresh();
            }
        }
        return oldOne.equals(newOne);
    }

    private <C extends AbstractConfig> String generateConfigId(C config) {
        String tagName = getTagName(config.getClass());
        int idx = configIdIndexes
                .computeIfAbsent(tagName, clazz -> new AtomicInteger(0))
                .incrementAndGet();
        return tagName + "#" + idx;
    }

    static <C extends AbstractConfig> Boolean isDefaultConfig(C config) {
        return config.isDefault();
    }

    public <C extends AbstractConfig> List<C> getDefaultConfigs(Class<C> cls) {
        Map<String, C> configsMap = getConfigsMap(cls);
        // find isDefault() == true
        List<C> list = configsMap.values().stream()
                .filter(c -> TRUE.equals(AbstractConfigManager.isDefaultConfig(c)))
                .collect(Collectors.toList());
        if (!list.isEmpty()) {
            return list;
        }

        // find isDefault() == null
        list = configsMap.values().stream()
                .filter(c -> AbstractConfigManager.isDefaultConfig(c) == null)
                .collect(Collectors.toList());
        return list;

        // exclude isDefault() == false
    }

    private <C extends AbstractConfig> Optional<C> checkUniqueConfig(DubboConfigWrapper<C> configWrapper, C config) {
        Map<String, C> configsMap = configWrapper.getConfigsMap();
        if (configsMap.isEmpty()) {
            return Optional.empty();
        }
        if (configWrapper.getDubboProperties().repeatable()) {
            return Optional.empty();
        }
        C oldOne = configsMap.values().iterator().next();
        String configName = oldOne.getClass().getSimpleName();
        String msgPrefix = "Duplicate Configs found for " + configName + ", only one unique " + configName
                + " is allowed for one application. previous: " + oldOne + ", later: " + config
                + ". According to config mode [" + configMode + "], ";
        switch (configMode) {
            case STRICT: {
                if (!isEquals(oldOne, config)) {
                    throw new IllegalStateException(msgPrefix + "please remove redundant configs and keep only one.");
                }
                break;
            }
            case IGNORE: {
                // ignore later config
                if (logger.isWarnEnabled() && duplicatedConfigs.add(config)) {
                    logger.warn(
                            COMMON_UNEXPECTED_EXCEPTION,
                            "",
                            "",
                            msgPrefix + "keep previous config and ignore later config");
                }
                return Optional.of(oldOne);
            }
            case OVERRIDE: {
                // clear previous config, add new config
                configsMap.clear();
                if (logger.isWarnEnabled() && duplicatedConfigs.add(config)) {
                    logger.warn(
                            COMMON_UNEXPECTED_EXCEPTION,
                            "",
                            "",
                            msgPrefix + "override previous config with later config");
                }
                break;
            }
            case OVERRIDE_ALL: {
                // override old one's properties with the new one
                oldOne.overrideWithConfig(config, true);
                if (logger.isWarnEnabled() && duplicatedConfigs.add(config)) {
                    logger.warn(
                            COMMON_UNEXPECTED_EXCEPTION,
                            "",
                            "",
                            msgPrefix + "override previous config with later config");
                }
                return Optional.of(oldOne);
            }
            case OVERRIDE_IF_ABSENT: {
                // override old one's properties with the new one
                oldOne.overrideWithConfig(config, false);
                if (logger.isWarnEnabled() && duplicatedConfigs.add(config)) {
                    logger.warn(
                            COMMON_UNEXPECTED_EXCEPTION,
                            "",
                            "",
                            msgPrefix + "override previous config with later config");
                }
                return Optional.of(oldOne);
            }
        }
        return Optional.empty();
    }

    public abstract void loadConfigs();

    public <T extends AbstractConfig> List<T> loadConfigsOfTypeFromProps(Class<T> cls) {
        List<T> tmpConfigs = new ArrayList<>();
        PropertiesConfiguration properties = environment.getPropertiesConfiguration();

        // load multiple configs with id
        Set<String> configIds = this.getConfigIdsFromProps(cls);
        configIds.forEach(id -> {
            if (!this.findConfig(cls, id).isPresent()) {
                T config;
                try {
                    config = createConfig(cls, scopeModel);
                    config.setId(id);
                } catch (Exception e) {
                    throw new IllegalStateException(
                            "create config instance failed, id: " + id + ", type:" + cls.getSimpleName());
                }

                String key = null;
                boolean addDefaultNameConfig = false;
                try {
                    // add default name config (same as id), e.g. dubbo.protocols.rest.port=1234
                    key = DUBBO + "." + AbstractConfig.getPluralTagName(cls) + "." + id + ".name";
                    if (properties.getProperty(key) == null) {
                        properties.setProperty(key, id);
                        addDefaultNameConfig = true;
                    }

                    config.refresh();
                    this.addConfig(config);
                    tmpConfigs.add(config);
                } catch (Exception e) {
                    logger.error(
                            COMMON_PROPERTY_TYPE_MISMATCH,
                            "",
                            "",
                            "load config failed, id: " + id + ", type:" + cls.getSimpleName(),
                            e);
                    throw new IllegalStateException("load config failed, id: " + id + ", type:" + cls.getSimpleName());
                } finally {
                    if (addDefaultNameConfig && key != null) {
                        properties.remove(key);
                    }
                }
            }
        });

        // If none config of the type, try load single config
        if (this.getRepeatableConfigs(cls).isEmpty()) {
            // load single config
            List<Map<String, String>> configurationMaps = environment.getConfigurationMaps();
            if (ConfigurationUtils.hasSubProperties(configurationMaps, AbstractConfig.getTypePrefix(cls))) {
                T config;
                try {
                    config = createConfig(cls, scopeModel);
                    config.refresh();
                } catch (Exception e) {
                    throw new IllegalStateException(
                            "create default config instance failed, type:" + cls.getSimpleName());
                }

                this.addConfig(config);
                tmpConfigs.add(config);
            }
        }

        return tmpConfigs;
    }

    private <T extends AbstractConfig> T createConfig(Class<T> cls, ScopeModel scopeModel)
            throws ReflectiveOperationException {
        T config = cls.getDeclaredConstructor().newInstance();
        config.setScopeModel(scopeModel);
        return config;
    }

    /**
     * Search props and extract config ids of specify type.
     * <pre>
     * # properties
     * dubbo.registries.registry1.address=xxx
     * dubbo.registries.registry2.port=xxx
     *
     * # extract
     * Set configIds = getConfigIds(RegistryConfig.class)
     *
     * # result
     * configIds: ["registry1", "registry2"]
     * </pre>
     *
     * @param clazz config type
     * @return ids of specify config type
     */
    private Set<String> getConfigIdsFromProps(Class<? extends AbstractConfig> clazz) {
        String prefix = CommonConstants.DUBBO + "." + AbstractConfig.getPluralTagName(clazz) + ".";
        return ConfigurationUtils.getSubIds(environment.getConfigurationMaps(), prefix);
    }

    protected <T extends AbstractConfig> void checkDefaultAndValidateConfigs(Class<T> configType) {
        try {
            if (shouldAddDefaultConfig(configType)) {
                T config = createConfig(configType, scopeModel);
                config.refresh();
                if (!isNeedValidation(config) || config.isValid()) {
                    this.addConfig(config);
                } else {
                    logger.info("Ignore invalid config: " + config);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Add default config failed: " + configType.getSimpleName(), e);
        }

        // validate configs
        Collection<T> configs = this.getRepeatableConfigs(configType);
        if (getConfigValidator() != null) {
            for (T config : configs) {
                getConfigValidator().validate(config);
            }
        }

        // check required default
        if (isRequired(configType) && configs.isEmpty()) {
            throw new IllegalStateException("Default config not found for " + configType.getSimpleName());
        }
    }

    /**
     * The component configuration that does not affect the main process does not need to be verified.
     *
     * @param config
     * @param <T>
     * @return
     */
    private <T extends AbstractConfig> boolean isNeedValidation(T config) {
        return !(config instanceof MetadataReportConfig);
    }

    private ConfigValidator getConfigValidator() {
        if (configValidator == null) {
            configValidator = applicationModel.getBeanFactory().getBean(ConfigValidator.class);
        }
        return configValidator;
    }

    /**
     * The configuration that does not affect the main process is not necessary.
     *
     * @param clazz
     * @param <T>
     * @return
     */
    private <T extends AbstractConfig> boolean isRequired(Class<T> clazz) {
        DubboProperties dubboProperties = clazz.getAnnotation(DubboProperties.class);
        if (dubboProperties == null) {
            throw new IllegalArgumentException("Unsupported config type: " + clazz);
        }
        return dubboProperties.required();
    }

    private <T extends AbstractConfig> boolean shouldAddDefaultConfig(Class<T> clazz) {
        // Configurations that are not required will not be automatically added to the default configuration
        if (!isRequired(clazz)) {
            return false;
        }
        return this.getDefaultConfigs(clazz).isEmpty();
    }

    public void refreshAll() {
        // refresh all configs here
        for (DubboConfigWrapper<?> value : configsCache.values()) {
            Collection<? extends AbstractConfig> configs = value.getConfigsMap().values();
            if (configs.isEmpty()) {
                continue;
            }
            if (value.getConfigType().isAssignableFrom(DubboApplicationConfig.class)) {
                if (configs.size() > 1) {
                    throw new IllegalStateException(
                            "Expected single instance of " + value.getConfigType() + ", but found " + configs.size()
                                    + " instances, please remove redundant configs. instances: " + configs);
                }
            }
            for (AbstractConfig config : configs) {
                config.refresh();
            }
        }
    }

    /**
     * In some scenario,  we may need to add and remove ServiceConfig or ReferenceConfig dynamically.
     *
     * @param config the config instance to remove.
     * @return
     */
    public <C extends AbstractConfig> boolean removeConfig(C config) {
        if (config == null) {
            return false;
        }

        DubboConfigWrapper<C> configs = (DubboConfigWrapper<C>) configsCache.get(getTagName(config.getClass()));
        if (configs != null) {
            // lock by config type
            synchronized (configs.getConfigsMap()) {
                return removeIfAbsent(config, configs.getConfigsMap());
            }
        }
        return false;
    }

    @Override
    public void destroy() throws IllegalStateException {
        clear();
    }

    public void clear() {
        this.configsCache.clear();
        this.configIdIndexes.clear();
        this.duplicatedConfigs.clear();
    }

    public boolean isInitialized() {
        return initialized.get();
    }
}
