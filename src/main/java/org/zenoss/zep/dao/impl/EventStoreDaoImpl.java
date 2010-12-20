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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.transaction.annotation.Transactional;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventSummaryRequest;
import org.zenoss.protobufs.zep.Zep.EventSummaryResult;
import org.zenoss.protobufs.zep.Zep.EventSummaryUpdate;
import org.zenoss.zep.EventContext;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventArchiveDao;
import org.zenoss.zep.dao.EventStoreDao;
import org.zenoss.zep.dao.EventSummaryDao;
import org.zenoss.zep.index.EventIndexDao;
import org.zenoss.zep.index.EventIndexer;

public class EventStoreDaoImpl implements EventStoreDao {
    private EventSummaryDao eventSummaryDao;
    private EventArchiveDao eventArchiveDao;
    private EventIndexDao eventSummaryIndexDao;
    private EventIndexDao eventArchiveIndexDao;
    private EventIndexer eventIndexer;

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

    public void setEventIndexer(EventIndexer eventIndexer) {
        this.eventIndexer = eventIndexer;
    }

    @Override
    @Transactional
    public void create(Event event, EventContext eventContext)
            throws ZepException {

        if (event.getSeverity() == EventSeverity.SEVERITY_CLEAR) {
            // If we have no clear classes - send event straight to archive
            if (eventContext.getClearClasses().isEmpty()) {
                this.eventArchiveDao.create(event);
                this.eventIndexer.markArchiveDirty();
            } else {
                this.eventSummaryDao.createClearEvent(event,
                        eventContext.getClearClasses());
                if (this.eventSummaryDao.clearEvents() > 0) {
                    this.eventIndexer.markSummaryDirty();
                }
            }
        } else {
            this.eventSummaryDao.create(event, eventContext.getStatus());
            this.eventIndexer.markSummaryDirty();
        }

    }

    @Override
    @Transactional
    public int delete(String uuid) throws ZepException {
        int rows = eventSummaryDao.delete(uuid);
        rows += eventArchiveDao.delete(uuid);
        eventSummaryIndexDao.delete(uuid);
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
    public EventSummaryResult list(EventSummaryRequest request)
            throws ZepException {
        return eventSummaryIndexDao.list(request);
    }

    @Override
    public EventSummaryResult listArchive(EventSummaryRequest request)
            throws ZepException {
        return eventArchiveIndexDao.list(request);
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

        int numUpdatedEvents = 0;

        EventStatus newStatus = update.getStatus();
        if (newStatus == null) {
            throw new ZepException("Required event status not specified");
        }
        switch (newStatus) {
        case STATUS_NEW:
            numUpdatedEvents = eventSummaryDao.reopen(uuids);
            break;
        case STATUS_ACKNOWLEDGED:
            numUpdatedEvents = eventSummaryDao.acknowledge(uuids,
                    update.getAcknowledgedByUserUuid());
            break;
        case STATUS_CLOSED:
            numUpdatedEvents = eventSummaryDao.close(uuids);
            break;
        case STATUS_SUPPRESSED:
            numUpdatedEvents = eventSummaryDao.suppress(uuids);
            break;
        default:
            throw new ZepException("Invalid status for update: " + newStatus);
        }

        if (numUpdatedEvents > 0) {
            eventIndexer.markSummaryDirty();
        }

        return numUpdatedEvents;
    }

    @Override
    @Transactional
    public int ageEvents(long agingInverval, TimeUnit unit,
            EventSeverity maxSeverity, int limit) throws ZepException {
        int numAged = eventSummaryDao.ageEvents(agingInverval, unit,
                maxSeverity, limit);
        if (numAged > 0) {
            eventIndexer.markSummaryDirty();
        }
        return numAged;
    }

    @Override
    @Transactional
    public int archive(long duration, TimeUnit unit, int limit)
            throws ZepException {
        int numArchived = this.eventSummaryDao.archive(duration, unit, limit);
        if (numArchived > 0) {
            eventIndexer.markArchiveDirty();
        }
        return numArchived;
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

    @Override
    public Map<String, Map<EventSeverity, Integer>> countSeverities(
            Set<String> tags) throws ZepException {
        return this.eventSummaryIndexDao.countSeverities(tags);
    }

    @Override
    public Map<String, EventSeverity> findWorstSeverity(Set<String> tags)
            throws ZepException {
        return this.eventSummaryIndexDao.findWorstSeverity(tags);
    }
}
