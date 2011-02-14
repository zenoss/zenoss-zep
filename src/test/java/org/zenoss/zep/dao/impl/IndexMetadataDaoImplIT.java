/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2011, Zenoss Inc.
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
import org.zenoss.zep.ZepException;
import org.zenoss.zep.ZepInstance;
import org.zenoss.zep.dao.IndexMetadata;
import org.zenoss.zep.dao.IndexMetadataDao;

import java.util.Random;

import static org.junit.Assert.*;

/**
 * Integration test for IndexMetadataDaoImpl.
 */
@ContextConfiguration({ "classpath:zep-config.xml" })
public class IndexMetadataDaoImplIT extends AbstractTransactionalJUnit4SpringContextTests {
    @Autowired
    public ZepInstance zepInstance;

    @Autowired
    public IndexMetadataDao indexMetadataDao;

    @Test
    public void testIndexMetadataDao() throws ZepException {
        byte[] sha1 = new byte[20];
        Random r = new Random();
        r.nextBytes(sha1);

        IndexMetadata md = new IndexMetadata();
        md.setIndexName("myindex");
        md.setIndexVersion(5);
        md.setIndexVersionHash(sha1);
        md.setLastIndexTime(System.currentTimeMillis());
        md.setLastCommitTime(md.getLastIndexTime());
        md.setZepInstance(zepInstance.getId());

        assertNull(indexMetadataDao.findIndexMetadata(md.getIndexName()));

        indexMetadataDao.updateIndexMetadata(md.getIndexName(), md.getIndexVersion(), md.getIndexVersionHash(),
                md.getLastIndexTime(), false);

        IndexMetadata mdFromDb = indexMetadataDao.findIndexMetadata(md.getIndexName());
        assertArrayEquals(md.getIndexVersionHash(), mdFromDb.getIndexVersionHash());
        assertEquals(md.getIndexName(), mdFromDb.getIndexName());
        assertEquals(md.getIndexVersion(), mdFromDb.getIndexVersion());
        assertEquals(0, mdFromDb.getLastCommitTime()); // isCommit was false
        assertEquals(md.getLastIndexTime(), mdFromDb.getLastIndexTime());
        assertEquals(md.getZepInstance(), mdFromDb.getZepInstance());

        md.setLastIndexTime(System.currentTimeMillis());
        md.setLastCommitTime(md.getLastIndexTime());
        indexMetadataDao.updateIndexMetadata(md.getIndexName(), md.getIndexVersion(), md.getIndexVersionHash(),
                md.getLastIndexTime(), true);

        mdFromDb = indexMetadataDao.findIndexMetadata(md.getIndexName());
        assertArrayEquals(md.getIndexVersionHash(), mdFromDb.getIndexVersionHash());
        assertEquals(md.getIndexName(), mdFromDb.getIndexName());
        assertEquals(md.getIndexVersion(), mdFromDb.getIndexVersion());
        assertEquals(md.getLastIndexTime(), mdFromDb.getLastCommitTime()); // isCommit was true
        assertEquals(md.getLastIndexTime(), mdFromDb.getLastIndexTime());
        assertEquals(md.getZepInstance(), mdFromDb.getZepInstance());
    }
}
