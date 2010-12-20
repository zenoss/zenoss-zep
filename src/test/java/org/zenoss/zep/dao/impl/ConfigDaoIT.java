/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2010, Zenoss Inc.
 * 
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 * 
 * For complete information please visit: http://www.zenoss.com/oss/
 */
package org.zenoss.zep.dao.impl;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.ConfigDao;

@ContextConfiguration({ "classpath:zep-config.xml" })
public class ConfigDaoIT extends AbstractTransactionalJUnit4SpringContextTests {
    @Autowired
    public ConfigDao configDao;

    @Test
    public void testConfig() throws ZepException {
        assertTrue(configDao.getConfig().isEmpty());

        Map<String, String> cfg = new HashMap<String, String>();
        cfg.put("name1", "val1");
        cfg.put("name2", "val2");
        configDao.setConfig(cfg);

        assertEquals(cfg, configDao.getConfig());
        assertEquals(cfg.get("name1"), configDao.getConfigValue("name1"));
        configDao.removeConfigValue("name1");
        assertNull(configDao.getConfigValue("name1"));
        cfg.remove("name1");
        cfg.remove("name2");
        cfg.put("name3", "val3");
        cfg.put("name4", "val4");
        configDao.setConfig(cfg);
        assertEquals(cfg, configDao.getConfig());

        configDao.setConfigValue("name3", "val33");
        assertEquals("val33", configDao.getConfigValue("name3"));
        configDao.setConfigValue("name5", "val5");
        assertEquals("val5", configDao.getConfigValue("name5"));
    }
}
