/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
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
        md.setZepInstance(zepInstance.getId());

        assertNull(indexMetadataDao.findIndexMetadata(md.getIndexName()));

        indexMetadataDao.updateIndexVersion(md.getIndexName(), md.getIndexVersion(), md.getIndexVersionHash());

        IndexMetadata mdFromDb = indexMetadataDao.findIndexMetadata(md.getIndexName());
        assertArrayEquals(md.getIndexVersionHash(), mdFromDb.getIndexVersionHash());
        assertEquals(md.getIndexName(), mdFromDb.getIndexName());
        assertEquals(md.getIndexVersion(), mdFromDb.getIndexVersion());
        assertEquals(md.getZepInstance(), mdFromDb.getZepInstance());
    }
}
