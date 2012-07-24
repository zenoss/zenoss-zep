/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.springframework.test.context.transaction.BeforeTransaction;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.DBMaintenanceService;

/**
 * Integration tests for DBMaintenanceService.
 */
@ContextConfiguration({ "classpath:zep-config.xml" })
public class DBMaintenanceServiceImplIT extends AbstractTransactionalJUnit4SpringContextTests {
    @Autowired
    public DBMaintenanceService dbMaintenanceService;

    @Test
    @BeforeTransaction
    public void testOptimizeTables() throws ZepException {
        // Just test that the optimize tables call works
        dbMaintenanceService.optimizeTables();
    }
}
