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

package org.apache.shardingsphere.shardingproxy;

import com.google.common.primitives.Ints;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.shardingsphere.api.config.RuleConfiguration;
import org.apache.shardingsphere.core.config.DataSourceConfiguration;
import org.apache.shardingsphere.core.constant.properties.ShardingPropertiesConstant;
import org.apache.shardingsphere.core.rule.Authentication;
import org.apache.shardingsphere.core.config.log.ConfigurationLogger;
import org.apache.shardingsphere.core.yaml.config.common.YamlAuthenticationConfiguration;
import org.apache.shardingsphere.core.yaml.swapper.impl.AuthenticationYamlSwapper;
import org.apache.shardingsphere.core.yaml.swapper.impl.EncryptRuleConfigurationYamlSwapper;
import org.apache.shardingsphere.core.yaml.swapper.impl.MasterSlaveRuleConfigurationYamlSwapper;
import org.apache.shardingsphere.core.yaml.swapper.impl.ShardingRuleConfigurationYamlSwapper;
import org.apache.shardingsphere.opentracing.ShardingTracer;
import org.apache.shardingsphere.orchestration.internal.registry.ShardingOrchestrationFacade;
import org.apache.shardingsphere.orchestration.yaml.swapper.OrchestrationConfigurationYamlSwapper;
import org.apache.shardingsphere.shardingproxy.backend.schema.LogicSchemas;
import org.apache.shardingsphere.shardingproxy.config.ShardingConfiguration;
import org.apache.shardingsphere.shardingproxy.config.ShardingConfigurationLoader;
import org.apache.shardingsphere.shardingproxy.config.yaml.YamlDataSourceParameter;
import org.apache.shardingsphere.shardingproxy.config.yaml.YamlProxyRuleConfiguration;
import org.apache.shardingsphere.shardingproxy.config.yaml.YamlProxyServerConfiguration;
import org.apache.shardingsphere.shardingproxy.context.ShardingProxyContext;
import org.apache.shardingsphere.shardingproxy.frontend.bootstrap.ShardingProxy;
import org.apache.shardingsphere.shardingproxy.util.DataSourceConverter;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Sharding-Proxy Bootstrap.
 *
 * @author zhangliang
 * @author wangkai
 * @author panjuan
 * @author sunbufu
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Bootstrap {
    
    private static final int DEFAULT_PORT = 3307;
    
    /**
     * Main entrance.
     *
     * @param args startup arguments
     * @throws IOException IO exception
     * @throws SQLException SQL exception
     */
    public static void main(final String[] args) throws IOException, SQLException {
        // 读取 /conf/server.yaml /conf/server-xxx.yaml 配置文件
        ShardingConfiguration shardingConfig = new ShardingConfigurationLoader().load();
        // log打印配置
        logRuleConfigurationMap(getRuleConfiguration(shardingConfig.getRuleConfigurationMap()).values());
        int port = getPort(args);
        // 是否有注册中心配置
        if (null == shardingConfig.getServerConfiguration().getOrchestration()) {
            startWithoutRegistryCenter(shardingConfig.getRuleConfigurationMap(), shardingConfig.getServerConfiguration().getAuthentication(), shardingConfig.getServerConfiguration().getProps(), port);
        } else {
            startWithRegistryCenter(shardingConfig.getServerConfiguration(), shardingConfig.getRuleConfigurationMap().keySet(), shardingConfig.getRuleConfigurationMap(), port);
        }
    }

    /**
     * @param args 参数
     * @return 没有参数返回3307 ，有参数返回第一个
     */
    private static int getPort(final String[] args) {
        if (0 == args.length) {
            return DEFAULT_PORT;
        }
        Integer paredPort = Ints.tryParse(args[0]);
        return paredPort == null ? DEFAULT_PORT : paredPort;
    }
    
    private static void startWithoutRegistryCenter(final Map<String, YamlProxyRuleConfiguration> ruleConfigs, // schemaName -> proxyRule
                                                   final YamlAuthenticationConfiguration authentication, final Properties prop, final int port) throws SQLException {
        // 授权信息
        Authentication authenticationConfiguration = getAuthentication(authentication);
        ConfigurationLogger.log(authenticationConfiguration);
        ConfigurationLogger.log(prop);
        // 授权信息静态变量绑定
        ShardingProxyContext.getInstance().init(authenticationConfiguration, prop);
        // 逻辑schema静态变量绑定
        LogicSchemas.getInstance().init(/*schemaName -> (数据源别名,数据源)*/getDataSourceParameterMap(ruleConfigs), getRuleConfiguration(ruleConfigs));
        initOpenTracing();
        ShardingProxy.getInstance().start(port);
    }
    
    private static void startWithRegistryCenter(final YamlProxyServerConfiguration serverConfig,
                                                final Collection<String> shardingSchemaNames, final Map<String, YamlProxyRuleConfiguration> ruleConfigs, final int port) {
        try (ShardingOrchestrationFacade shardingOrchestrationFacade = new ShardingOrchestrationFacade(
                new OrchestrationConfigurationYamlSwapper().swap(serverConfig.getOrchestration()), shardingSchemaNames)) {
            initShardingOrchestrationFacade(serverConfig, ruleConfigs, shardingOrchestrationFacade);
            Authentication authentication = shardingOrchestrationFacade.getConfigService().loadAuthentication();
            Properties properties = shardingOrchestrationFacade.getConfigService().loadProperties();
            ConfigurationLogger.log(authentication);
            ConfigurationLogger.log(properties);
            ShardingProxyContext.getInstance().init(authentication, properties);
            LogicSchemas.getInstance().init(shardingSchemaNames, getSchemaDataSourceParameterMap(shardingOrchestrationFacade), getSchemaRules(shardingOrchestrationFacade), true);
            initOpenTracing();
            ShardingProxy.getInstance().start(port);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    private static Map<String, Map<String, YamlDataSourceParameter>> getSchemaDataSourceParameterMap(final ShardingOrchestrationFacade shardingOrchestrationFacade) {
        Map<String, Map<String, YamlDataSourceParameter>> result = new LinkedHashMap<>();
        for (String each : shardingOrchestrationFacade.getConfigService().getAllShardingSchemaNames()) {
            result.put(each, DataSourceConverter.getDataSourceParameterMap(shardingOrchestrationFacade.getConfigService().loadDataSourceConfigurations(each)));
        }
        return result;
    }
    
    private static Map<String, RuleConfiguration> getSchemaRules(final ShardingOrchestrationFacade shardingOrchestrationFacade) {
        Map<String, RuleConfiguration> result = new LinkedHashMap<>();
        for (String each : shardingOrchestrationFacade.getConfigService().getAllShardingSchemaNames()) {
            if (shardingOrchestrationFacade.getConfigService().isEncryptRule(each)) {
                result.put(each, shardingOrchestrationFacade.getConfigService().loadEncryptRuleConfiguration(each));
            } else if (shardingOrchestrationFacade.getConfigService().isShardingRule(each)) {
                result.put(each, shardingOrchestrationFacade.getConfigService().loadShardingRuleConfiguration(each));
            } else {
                result.put(each, shardingOrchestrationFacade.getConfigService().loadMasterSlaveRuleConfiguration(each));
            }
        }
        return result;
    }
    
    private static void initShardingOrchestrationFacade(
            final YamlProxyServerConfiguration serverConfig, final Map<String, YamlProxyRuleConfiguration> ruleConfigs, final ShardingOrchestrationFacade shardingOrchestrationFacade) {
        if (ruleConfigs.isEmpty()) {
            shardingOrchestrationFacade.init();
        } else {
            shardingOrchestrationFacade.init(getDataSourceConfigurationMap(ruleConfigs),
                    getRuleConfiguration(ruleConfigs), getAuthentication(serverConfig.getAuthentication()), serverConfig.getProps());
        }
    }
    
    private static void initOpenTracing() {
        if (ShardingProxyContext.getInstance().getShardingProperties().<Boolean>getValue(ShardingPropertiesConstant.PROXY_OPENTRACING_ENABLED)) {
            ShardingTracer.init();
        }
    }
    
    private static Map<String, Map<String, DataSourceConfiguration>> getDataSourceConfigurationMap(final Map<String, YamlProxyRuleConfiguration> ruleConfigs) {
        Map<String, Map<String, DataSourceConfiguration>> result = new LinkedHashMap<>();
        for (Entry<String, YamlProxyRuleConfiguration> entry : ruleConfigs.entrySet()) {
            result.put(entry.getKey(), DataSourceConverter.getDataSourceConfigurationMap(entry.getValue().getDataSources()));
        }
        return result;
    }
    
    private static Map<String, Map<String, YamlDataSourceParameter>> getDataSourceParameterMap(final Map<String, YamlProxyRuleConfiguration> localRuleConfigs) {
        Map<String, Map<String, YamlDataSourceParameter>> result = new HashMap<>(localRuleConfigs.size(), 1);
        for (Entry<String, YamlProxyRuleConfiguration> entry : localRuleConfigs.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getDataSources());
        }
        return result;
    }
    
    private static Map<String, RuleConfiguration> getRuleConfiguration(final Map<String, YamlProxyRuleConfiguration> localRuleConfigs) {
        Map<String, RuleConfiguration> result = new HashMap<>();
        // 解析每个server-xxx.yaml 文件，生成多个规则配置
        for (Entry<String, YamlProxyRuleConfiguration> entry : localRuleConfigs.entrySet()) {
            // 这里可以看出，如果同时配置 shardingRule masterSlaveRule encryptRule ，会按照shardingRule masterSlaveRule encryptRule优先级，生效一个
            if (null != entry.getValue().getShardingRule()) {
                result.put(entry.getKey(), new ShardingRuleConfigurationYamlSwapper().swap(entry.getValue().getShardingRule()));
            } else if (null != entry.getValue().getMasterSlaveRule()) {
                result.put(entry.getKey(), new MasterSlaveRuleConfigurationYamlSwapper().swap(entry.getValue().getMasterSlaveRule()));
            } else if (null != entry.getValue().getEncryptRule()) {
                result.put(entry.getKey(), new EncryptRuleConfigurationYamlSwapper().swap(entry.getValue().getEncryptRule()));
            }
        }
        return result;
    }
    
    private static Authentication getAuthentication(final YamlAuthenticationConfiguration yamlAuthenticationConfig) {
        return new AuthenticationYamlSwapper().swap(yamlAuthenticationConfig);
    }
    
    /**
     * Log rule configurations.
     *
     * @param ruleConfigurations log rule configurations
     */
    private static void logRuleConfigurationMap(final Collection<RuleConfiguration> ruleConfigurations) {
        if (CollectionUtils.isNotEmpty(ruleConfigurations)) {
            for (RuleConfiguration each : ruleConfigurations) {
                ConfigurationLogger.log(each);
            }
        }
    }
}
