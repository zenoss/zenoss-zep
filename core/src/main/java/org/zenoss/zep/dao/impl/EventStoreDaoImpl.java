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

import org.springframework.transaction.annotation.Transactional;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventDetailSet;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventSummaryUpdate;
import org.zenoss.zep.EventContext;
import org.zenoss.zep.ZepException;
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
    private EventIndexDao eventSummaryIndexDao;
    private EventIndexDao eventArchiveIndexDao;

    public EventStoreDaoImpl() {
    }

    public void setEventSummaryDao(EventSummaryDao eventSummaryDao) {
        this.eventSummaryDao = eventSummaryDao;
    }

    public void setEventArchiveDao(EventArchiveDao eventArchiveDao) {
        this.eventArchiveDao = eventArchiveDao;
    }

    public void setEventSummaryIndexDao(EventIndexDao eventSummaryIndexDao) {
        this.eventSummaryIndexDao = eventSummaryIndexDao;
    }

    public void setEventArchiveIndexDao(EventIndexDao eventIndexDao) {
        this.eventArchiveIndexDao = eventIndexDao;
    }

    @Override
    @Transactional
    public String create(Event event, EventContext eventContext) throws ZepException {
        final String uuid;
        if (event.getSeverity() == EventSeverity.SEVERITY_CLEAR) {
            uuid = this.eventSummaryDao.createClearEvent(event, eventContext.getClearClasses());
        } else {
            uuid = this.eventSummaryDao.create(event, eventContext.getStatus());
        }
        return uuid;
    }

    @Override
    @Transactional
    public int delete(String uuid) throws ZepException {
        int rows = eventSummaryDao.delete(uuid);
        if (rows > 0) {
            eventSummaryIndexDao.delete(uuid);
        }
        else {
            rows = eventArchiveDao.delete(uuid);
            if (rows > 0) {
                eventArchiveIndexDao.delete(uuid);
            }
        }
        return rows;
    }

    @Override
    @Transactional(readOnly = true)
    public EventSummary findByUuid(String uuid) throws ZepException {
        EventSummary summary = eventSummaryDao.findByUuid(uuid);
        if (summary == null) {
            summary = eventArchiveDao.findByUuid(uuid);
        }
        return summary;
    }

    @Override
    @Transactional
    public int update(String uuid, EventSummaryUpdate update)
            throws ZepException {
        return update(Collections.singletonList(uuid), update);
    }

    @Override
    @Transactional
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
    @Transactional
    public int updateDetails(String uuid, EventDetailSet details)
            throws ZepException {
        int numRows = eventSummaryDao.updateDetails(uuid, details);
        if (numRows == 0) {
            numRows = eventArchiveDao.updateDetails(uuid, details);
        }
        return numRows;
    }

    @Override
    @Transactional
    public int ageEvents(long agingInterval, TimeUnit unit,
            EventSeverity maxSeverity, int limit) throws ZepException {
        return eventSummaryDao.ageEvents(agingInterval, unit, maxSeverity, limit);
    }

    @Override
    @Transactional
    public int archive(long duration, TimeUnit unit, int limit)
            throws ZepException {
        return this.eventSummaryDao.archive(duration, unit, limit);
    }

    @Override
    @Transactional
    public int addNote(String uuid, EventNote note) throws ZepException {
        int numRows = eventSummaryDao.addNote(uuid, note);
        if (numRows == 0) {
            numRows = eventArchiveDao.addNote(uuid, note);
        }
        return numRows;
    }

    @Override
    @Transactional
    public void purge(int duration, TimeUnit unit) throws ZepException {
        eventArchiveDao.purge(duration, unit);
        eventArchiveIndexDao.purge(duration, unit);
    }
}