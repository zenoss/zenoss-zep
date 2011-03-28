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

package org.zenoss.zep.index.impl;

import org.apache.lucene.index.IndexWriter;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.index.EventIndexDao;

import java.io.IOException;

/**
 * EventIndexDaoImpl for event archive. Overrides the {@link #stage(EventSummary)} method
 * to delete all indexed events from the summary index.
 */
public class EventArchiveIndexDaoImpl extends EventIndexDaoImpl {

    private final EventIndexDao deleteFromDao;

    public EventArchiveIndexDaoImpl(String name, IndexWriter writer, EventIndexDao deleteFromDao)
            throws IOException {
        super(name, writer);
        this.deleteFromDao = deleteFromDao;
    }

    @Override
    public void stage(EventSummary event) throws ZepException {
        this.deleteFromDao.stageDelete(event.getUuid());
        super.stage(event);
    }

    @Override
    public void commit(boolean forceOptimize) throws ZepException {
        this.deleteFromDao.commit(forceOptimize);
        super.commit(forceOptimize);
    }
}
