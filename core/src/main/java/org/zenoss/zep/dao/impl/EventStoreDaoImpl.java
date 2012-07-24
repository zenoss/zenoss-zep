/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import org.zenoss.protobufs.zep.Zep.EventDetailSet;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventSummaryUpdate;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalReadOnly;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventArchiveDao;
import org.zenoss.zep.dao.EventStoreDao;
import org.zenoss.zep.dao.EventSummaryDao;
import org.zenoss.zep.index.EventIndexDao;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class EventStoreDaoImpl implements EventStoreDao {
    private EventSummaryDao eventSummaryDao;
    private EventArchiveDao eventArchiveDao;
    private EventIndexDao eventArchiveIndexDao;

    public EventStoreDaoImpl() {
    }

    public void setEventSummaryDao(EventSummaryDao eventSummaryDao) {
        this.eventSummaryDao = eventSummaryDao;
    }

    public void setEventArchiveDao(EventArchiveDao eventArchiveDao) {
        this.eventArchiveDao = eventArchiveDao;
    }

    public void setEventArchiveIndexDao(EventIndexDao eventIndexDao) {
        this.eventArchiveIndexDao = eventIndexDao;
    }

    @Override
    @TransactionalReadOnly
    public EventSummary findByUuid(String uuid) throws ZepException {
        EventSummary summary = eventSummaryDao.findByUuid(uuid);
        if (summary == null) {
            summary = eventArchiveDao.findByUuid(uuid);
        }
        return summary;
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int update(String uuid, EventSummaryUpdate update)
            throws ZepException {
        return update(Collections.singletonList(uuid), update);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int update(List<String> uuids, EventSummaryUpdate update)
            throws ZepException {

        final int numUpdatedEvents;

        EventStatus newStatus = update.getStatus();
        if (newStatus == null) {
            throw new ZepException("Required event status not specified");
        }
        switch (newStatus) {
        case STATUS_NEW:
            numUpdatedEvents = eventSummaryDao.reopen(uuids,
                    update.getCurrentUserUuid(), update.getCurrentUserName());
            break;
        case STATUS_ACKNOWLEDGED:
            numUpdatedEvents = eventSummaryDao.acknowledge(uuids,
                    update.getCurrentUserUuid(), update.getCurrentUserName());
            break;
        case STATUS_CLOSED:
            numUpdatedEvents = eventSummaryDao.close(uuids,
                    update.getCurrentUserUuid(), update.getCurrentUserName());
            break;
        case STATUS_SUPPRESSED:
            numUpdatedEvents = eventSummaryDao.suppress(uuids);
            break;
        default:
            throw new ZepException("Invalid status for update: " + newStatus);
        }

        return numUpdatedEvents;
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int updateDetails(String uuid, EventDetailSet details)
            throws ZepException {
        int numRows = eventSummaryDao.updateDetails(uuid, details);
        if (numRows == 0) {
            numRows = eventArchiveDao.updateDetails(uuid, details);
        }
        return numRows;
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int ageEvents(long agingInterval, TimeUnit unit,
            EventSeverity maxSeverity, int limit, boolean inclusiveSeverity) throws ZepException {
        return eventSummaryDao.ageEvents(agingInterval, unit, maxSeverity, limit, inclusiveSeverity);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int archive(long duration, TimeUnit unit, int limit)
            throws ZepException {
        return this.eventSummaryDao.archive(duration, unit, limit);
    }

    @Override
    @TransactionalRollbackAllExceptions
    public int addNote(String uuid, EventNote note) throws ZepException {
        int numRows = eventSummaryDao.addNote(uuid, note);
        if (numRows == 0) {
            numRows = eventArchiveDao.addNote(uuid, note);
        }
        return numRows;
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void purge(int duration, TimeUnit unit) throws ZepException {
        eventArchiveDao.purge(duration, unit);
        eventArchiveIndexDao.purge(duration, unit);
    }
}
