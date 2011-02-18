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

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.ZepConfig;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.ConfigDao;

import static org.junit.Assert.*;

@ContextConfiguration({"classpath:zep-config.xml"})
public class ConfigDaoIT extends AbstractTransactionalJUnit4SpringContextTests {
    @Autowired
    public ConfigDao configDao;

    @Test
    public void testConfig() throws ZepException {
        ZepConfig.Builder builder = ZepConfig.newBuilder();
        builder.setEventAgeDisableSeverity(EventSeverity.SEVERITY_CRITICAL);
        builder.setEventAgeIntervalMinutes(60);
        builder.setEventArchiveIntervalDays(7);
        builder.setEventArchivePurgeIntervalDays(90);
        builder.setEventOccurrencePurgeIntervalDays(30);
        ZepConfig cnf = builder.build();
        configDao.setConfig(cnf);

        assertEquals(cnf, configDao.getConfig());

        cnf = ZepConfig.newBuilder(cnf).setEventAgeIntervalMinutes(90).build();
        configDao.setConfigValue("event_age_interval_minutes", cnf);
        assertEquals(cnf, configDao.getConfig());

        assertEquals(1, configDao.removeConfigValue("event_age_interval_minutes"));
        assertEquals(0, configDao.removeConfigValue("event_age_interval_minutes"));
        assertEquals(ZepConfig.getDefaultInstance().getEventAgeIntervalMinutes(),
                     configDao.getConfig().getEventAgeIntervalMinutes());
    }
}
