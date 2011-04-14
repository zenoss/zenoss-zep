/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.zenoss.zep.EventPlugin;
import org.zenoss.zep.EventPostProcessingPlugin;
import org.zenoss.zep.EventPreProcessingPlugin;
import org.zenoss.zep.PluginService;

/**
 * {@link PluginService} implementation which supports loading plug-ins from a
 * Spring {@link ApplicationContext} as well as from jars on the classpath using
 * a {@link ServiceLoader}.
 */
public class PluginServiceImpl implements PluginService,
        ApplicationListener<ContextRefreshedEvent> {

    private static final Logger logger = LoggerFactory
            .getLogger(PluginServiceImpl.class);

    private List<EventPreProcessingPlugin> preProcessingPlugins = new ArrayList<EventPreProcessingPlugin>();

    private List<EventPostProcessingPlugin> postProcessingPlugins = new ArrayList<EventPostProcessingPlugin>();

    private Properties pluginProperties = new Properties();

    public PluginServiceImpl() {
    }

    public void setPluginProperties(Properties pluginProperties) {
        this.pluginProperties = pluginProperties;
    }

    private static class PluginConfig {
        private final Map<String, Map<String, String>> allPluginProperties = new HashMap<String, Map<String, String>>();

        public Map<String, String> getPluginProperties(String pluginId) {
            Map<String, String> pluginProps = allPluginProperties.get(pluginId);
            if (pluginProps == null) {
                pluginProps = Collections.emptyMap();
            }
            return pluginProps;
        }

        public void setPluginProperty(String pluginId, String name, String value) {
            Map<String, String> pluginProps = allPluginProperties.get(pluginId);
            if (pluginProps == null) {
                pluginProps = new HashMap<String, String>();
                allPluginProperties.put(pluginId, pluginProps);
            }
            pluginProps.put(name, value);
        }
    }

    private PluginConfig loadPluginConfig(Properties properties) {
        final PluginConfig cfg = new PluginConfig();
        final Pattern pattern = Pattern.compile("plugin\\.([^\\.]+)\\.(.+)");
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            final String key = (String) entry.getKey();
            final String val = (String) entry.getValue();
            final Matcher matcher = pattern.matcher(key);
            if (matcher.matches()) {
                cfg.setPluginProperty(matcher.group(1), matcher.group(2), val);
            }
        }
        return cfg;
    }

    private Set<String> getEnabledPlugins() {
        final Set<String> enabledPlugins = new HashSet<String>();
        final String enabledPluginsProp = this.pluginProperties
                .getProperty("zep.plugins");
        if (enabledPluginsProp != null) {
            String[] userPluginsArray = enabledPluginsProp.split(",");
            for (String userPlugin : userPluginsArray) {
                String userPluginId = userPlugin.trim();
                if (userPluginId.length() > 0) {
                    enabledPlugins.add(userPluginId);
                }
            }
        }
        return enabledPlugins;
    }

    public static class MissingDependencyException extends Exception {
        private static final long serialVersionUID = 1L;
        private final String pluginId;
        private final String dependencyId;

        public MissingDependencyException(String pluginId, String dependencyId) {
            this.pluginId = pluginId;
            this.dependencyId = dependencyId;
        }

        public String getPluginId() {
            return pluginId;
        }

        public String getDependencyId() {
            return dependencyId;
        }
    }

    public static class DependencyCycleException extends Exception {
        private static final long serialVersionUID = 1L;
        private final String pluginId;

        public DependencyCycleException(String pluginId) {
            this.pluginId = pluginId;
        }

        public String getPluginId() {
            return pluginId;
        }
    }

    /**
     * Recursive method used to find plug-in cycles.
     * 
     * @param plugins
     *            The plug-ins to analyze.
     * @param existingDeps
     *            Any existing dependencies which will be scanned to avoid
     *            {@link MissingDependencyException} from being thrown.
     * @param plugin
     *            The plug-in which is currently being analyzed.
     * @param analyzed
     *            The previously analyzed plug-ins which are not analyzed again.
     * @param dependencies
     *            The current dependency chain which is being analyzed.
     * @throws MissingDependencyException
     *             If a missing dependency is discovered.
     * @throws DependencyCycleException
     *             If a cycle in dependencies is detected.
     */
    private static void detectPluginCycles(
            Map<String, ? extends EventPlugin> plugins,
            Set<String> existingDeps, EventPlugin plugin, Set<String> analyzed,
            Set<String> dependencies) throws MissingDependencyException,
            DependencyCycleException {
        // Don't detect cycles again on the same plug-in.
        if (!analyzed.add(plugin.getId())) {
            return;
        }
        dependencies.add(plugin.getId());
        for (String dependencyId : plugin.getDependencies()) {
            EventPlugin dependentPlugin = plugins.get(dependencyId);
            if (dependentPlugin == null) {
                if (!existingDeps.contains(dependencyId)) {
                    throw new MissingDependencyException(plugin.getId(),
                            dependencyId);
                }
            } else {
                if (dependencies.contains(dependencyId)) {
                    throw new DependencyCycleException(dependencyId);
                }
                detectPluginCycles(plugins, existingDeps, dependentPlugin,
                        analyzed, dependencies);
            }
        }
        dependencies.remove(plugin.getId());
    }

    /**
     * Detects any cycles in the specified plug-ins and their dependencies.
     * 
     * @param plugins
     *            The plug-ins to analyze.
     * @param existingDeps
     *            Any existing dependency ids which don't trigger a
     *            {@link MissingDependencyException}.
     * @throws MissingDependencyException
     *             If a dependency is not found.
     * @throws DependencyCycleException
     *             If a cycle in the dependencies is detected.
     */
    static void detectCycles(Map<String, ? extends EventPlugin> plugins,
            Set<String> existingDeps) throws MissingDependencyException,
            DependencyCycleException {
        Set<String> analyzed = new HashSet<String>();
        for (EventPlugin plugin : plugins.values()) {
            detectPluginCycles(plugins, existingDeps, plugin, analyzed,
                    new HashSet<String>());
        }
    }

    /**
     * Sorts plug-ins in the order of their dependencies, so all dependencies
     * come before the plug-in which depends on them.
     * 
     * @param <T>
     *            The type of {@link EventPlugin}.
     * @param plugins
     *            Map of plug-ins keyed by the plug-in ID.
     * @return A sorted list of plug-ins in order based on their dependencies.
     */
    static <T extends EventPlugin> List<T> sortPluginsByDependencies(
            Map<String, T> plugins) {
        List<T> sorted = new ArrayList<T>(plugins.size());
        Map<String, T> mutablePlugins = new HashMap<String, T>(plugins);
        while (!mutablePlugins.isEmpty()) {
            for (Iterator<T> it = mutablePlugins.values().iterator(); it
                    .hasNext();) {
                T plugin = it.next();
                boolean allDependenciesResolved = true;
                for (String dep : plugin.getDependencies()) {
                    T depPlugin = mutablePlugins.get(dep);
                    if (depPlugin != null) {
                        allDependenciesResolved = false;
                        break;
                    }
                }
                if (allDependenciesResolved) {
                    sorted.add(plugin);
                    it.remove();
                }
            }
        }
        return sorted;
    }

    private void loadPlugins(ApplicationContext context) {
        Set<String> enabledPlugins = getEnabledPlugins();
        Set<String> allPluginIds = new HashSet<String>();
        Map<String, EventPreProcessingPlugin> availablePreProcessingPlugins = new HashMap<String, EventPreProcessingPlugin>();
        Map<String, EventPostProcessingPlugin> availablePostProcessingPlugins = new HashMap<String, EventPostProcessingPlugin>();

        // Load plug-ins from Spring.
        Map<String, EventPlugin> pluginsFromSpring = context
                .getBeansOfType(EventPlugin.class);
        for (EventPlugin plugin : pluginsFromSpring.values()) {
            if (!enabledPlugins.contains(plugin.getId())) {
                logger.info("Plugin {} is disabled", plugin.getId());
            } else if (!allPluginIds.add(plugin.getId())) {
                logger.warn("Multiple plugins with id {} found", plugin.getId());
            } else if (plugin instanceof EventPreProcessingPlugin) {
                availablePreProcessingPlugins.put(plugin.getId(),
                        (EventPreProcessingPlugin) plugin);
            } else if (plugin instanceof EventPostProcessingPlugin) {
                availablePostProcessingPlugins.put(plugin.getId(),
                        (EventPostProcessingPlugin) plugin);
            }
        }

        // Load plug-ins from ServiceLoader.
        final ServiceLoader<EventPlugin> eventPlugins = ServiceLoader
                .load(EventPlugin.class);
        for (EventPlugin plugin : eventPlugins) {
            try {
                if (!enabledPlugins.contains(plugin.getId())) {
                    logger.info("Plugin {} is disabled", plugin.getId());
                } else if (!allPluginIds.add(plugin.getId())) {
                    logger.warn("Multiple plugins with id {} found", plugin.getId());
                } else if (plugin instanceof EventPreProcessingPlugin) {
                    availablePreProcessingPlugins.put(plugin.getId(),
                            (EventPreProcessingPlugin) plugin);
                } else if (plugin instanceof EventPostProcessingPlugin) {
                    availablePostProcessingPlugins.put(plugin.getId(),
                            (EventPostProcessingPlugin) plugin);
                }
            } catch (ServiceConfigurationError e) {
                logger.warn("Failed to load plug-in", e);
            }
        }
        try {
            // Detect cycles or missing dependencies in pre-processing plug-ins.
            detectCycles(availablePreProcessingPlugins,
                    Collections.<String> emptySet());

            // Detect cycles or missing dependencies in post-processing plugins.
            // Post-processing plug-ins can depend on pre-processing plug-ins.
            detectCycles(availablePostProcessingPlugins,
                    availablePreProcessingPlugins.keySet());

            PluginConfig pluginCfg = loadPluginConfig(this.pluginProperties);

            this.preProcessingPlugins = sortPluginsByDependencies(availablePreProcessingPlugins);
            for (EventPlugin plugin : this.preProcessingPlugins) {
                logger.info("Initializing plug-in: {}", plugin.getId());
                plugin.init(pluginCfg.getPluginProperties(plugin.getId()));
            }

            this.postProcessingPlugins = sortPluginsByDependencies(availablePostProcessingPlugins);
            for (EventPlugin plugin : this.postProcessingPlugins) {
                logger.info("Initializing plug-in: {}", plugin.getId());
                plugin.init(pluginCfg.getPluginProperties(plugin.getId()));
            }
        } catch (MissingDependencyException e) {
            logger.warn(
                    "Invalid plugin configuration. Missing dependency {} for plug-in {}",
                    e.getDependencyId(), e.getPluginId());
        } catch (DependencyCycleException e) {
            logger.warn(
                    "Invalid plug-in configuration. Cycle detected on plug-in {}",
                    e.getPluginId());
        }
    }

    @Override
    public List<EventPreProcessingPlugin> getPreProcessingPlugins() {
        return Collections.unmodifiableList(this.preProcessingPlugins);
    }

    @Override
    public List<EventPostProcessingPlugin> getPostProcessingPlugins() {
        return Collections.unmodifiableList(this.postProcessingPlugins);
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        loadPlugins(event.getApplicationContext());
    }
}
