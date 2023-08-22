/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.impl;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.plugins.EventPostIndexContext;
import org.zenoss.zep.plugins.EventPostIndexPlugin;
import org.zenoss.zep.plugins.EventPreCreateContext;
import org.zenoss.zep.plugins.EventPreCreatePlugin;
import org.zenoss.zep.plugins.exceptions.DependencyCycleException;
import org.zenoss.zep.plugins.exceptions.MissingDependencyException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.*;

@ContextConfiguration("classpath:zep-test-plugins.xml")
public class PluginServiceImplTest extends AbstractJUnit4SpringContextTests {

    @Autowired
    public PluginServiceImpl pluginService;

    @Test
    public void testInit() throws IOException {
        Properties properties = new Properties();
        InputStream is = null;
        try {
            is = getClass().getResourceAsStream("/test-zeneventserver.conf");
            properties.load(is);
        } finally {
            if (is != null) {
                is.close();
            }
        }
        pluginService.initializePlugins();
        List<EventPreCreatePlugin> prePlugins = pluginService.getPluginsByType(EventPreCreatePlugin.class);
        assertEquals(3, prePlugins.size());
        List<EventPostIndexPlugin> postPlugins = pluginService.getPluginsByType(EventPostIndexPlugin.class);
        assertEquals(2, postPlugins.size());
        /* Verify order */
        assertEquals("MyPrePlugin3", prePlugins.get(0).getId());
        assertEquals("MyPrePlugin2", prePlugins.get(1).getId());
        assertEquals(properties.getProperty("plugin.MyPrePlugin2.prop2"),
                prePlugins.get(1).getProperties().get("prop2"));
        assertEquals("MyPrePlugin1", prePlugins.get(2).getId());
        assertEquals("MyPostPlugin2", postPlugins.get(0).getId());
        assertEquals("MyPostPlugin1", postPlugins.get(1).getId());
        assertEquals(properties.getProperty("plugin.MyPostPlugin1.myprop.name"),
                postPlugins.get(1).getProperties().get("myprop.name"));
    }

    @Test
    public void testCycleDetection() throws MissingDependencyException {
        EventPreCreatePlugin plugin1 = new EventPreCreatePlugin() {
            @Override
            public String getId() {
                return "Plugin1";
            }

            @Override
            public Set<String> getDependencies() {
                return Collections.singleton("Plugin2");
            }

            @Override
            public Event processEvent(Event event, EventPreCreateContext ctx)
                    throws ZepException {
                return event;
            }
        };
        EventPreCreatePlugin plugin2 = new EventPreCreatePlugin() {
            @Override
            public String getId() {
                return "Plugin2";
            }

            @Override
            public Set<String> getDependencies() {
                return Collections.singleton("Plugin3");
            }

            @Override
            public Event processEvent(Event event, EventPreCreateContext ctx)
                    throws ZepException {
                return event;
            }
        };
        EventPreCreatePlugin plugin3 = new EventPreCreatePlugin() {
            @Override
            public String getId() {
                return "Plugin3";
            }

            @Override
            public Set<String> getDependencies() {
                return Collections.singleton("Plugin1");
            }

            @Override
            public Event processEvent(Event event, EventPreCreateContext ctx)
                    throws ZepException {
                return event;
            }
        };
        Map<String, EventPreCreatePlugin> plugins = new HashMap<String, EventPreCreatePlugin>();
        plugins.put(plugin1.getId(), plugin1);
        plugins.put(plugin2.getId(), plugin2);
        plugins.put(plugin3.getId(), plugin3);
        try {
            PluginServiceImpl.detectCycles(plugins,
                    Collections.<String> emptySet());
            fail("Expected to fail with a cycle exception");
        } catch (DependencyCycleException e) {
            /* Expected */
        }
    }

    @Test
    public void testMissingDependency() throws DependencyCycleException,
            MissingDependencyException {
        EventPreCreatePlugin plugin1 = new EventPreCreatePlugin() {
            @Override
            public String getId() {
                return "Plugin1";
            }

            @Override
            public Set<String> getDependencies() {
                return Collections.singleton("PluginNotFound");
            }

            @Override
            public Event processEvent(Event event, EventPreCreateContext ctx)
                    throws ZepException {
                return event;
            }
        };
        try {
            PluginServiceImpl.detectCycles(Collections
                    .<String, EventPreCreatePlugin> singletonMap(
                            plugin1.getId(), plugin1), Collections
                    .<String> emptySet());
            fail("Expected MissingDependencyException");
        } catch (MissingDependencyException e) {
            assertEquals(plugin1.getId(), e.getPluginId());
            assertEquals(plugin1.getDependencies().iterator().next(),
                    e.getDependencyId());
        }
        /*
         * Verify if we pass in the plug-in as an existing dependency no error
         * is thrown.
         */
        PluginServiceImpl.detectCycles(Collections
                .<String, EventPreCreatePlugin> singletonMap(
                        plugin1.getId(), plugin1),
                Collections.<String> singleton(plugin1.getDependencies()
                        .iterator().next()));
    }

    public static final class MyPrePlugin1 extends EventPreCreatePlugin {
        @Override
        public String getId() {
            return getClass().getSimpleName();
        }

        @Override
        public Event processEvent(Event event, EventPreCreateContext ctx)
                throws ZepException {
            return event;
        }

        @Override
        public Set<String> getDependencies() {
            return Collections.singleton("MyPrePlugin2");
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + this.properties + "]";
        }
    }

    public static final class MyPrePlugin2 extends EventPreCreatePlugin {
        @Override
        public String getId() {
            return getClass().getSimpleName();
        }

        @Override
        public Event processEvent(Event event, EventPreCreateContext ctx)
                throws ZepException {
            return event;
        }

        @Override
        public Set<String> getDependencies() {
            return Collections.singleton("MyPrePlugin3");
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + this.properties + "]";
        }
    }

    public static final class MyPrePlugin3 extends EventPreCreatePlugin {
        @Override
        public Event processEvent(Event event, EventPreCreateContext ctx)
                throws ZepException {
            return event;
        }
    }

    public static final class MyPostPlugin1 extends EventPostIndexPlugin {
        @Override
        public Set<String> getDependencies() {
            return new HashSet<String>(Arrays.asList("MyPrePlugin1", "MyPostPlugin2"));
        }

        @Override
        public void processEvent(EventSummary eventSummary, EventPostIndexContext context)
                throws ZepException {
        }
    }

    public static final class MyPostPlugin2 extends EventPostIndexPlugin {
        @Override
        public void processEvent(EventSummary eventSummary, EventPostIndexContext context)
                throws ZepException {
        }
    }
}
