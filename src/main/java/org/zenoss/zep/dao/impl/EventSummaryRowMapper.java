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

import static org.zenoss.zep.dao.impl.EventConstants.*;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import org.springframework.jdbc.core.RowMapper;
import org.zenoss.protobufs.JsonFormat;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;

public class EventSummaryRowMapper implements RowMapper<EventSummary> {
    private final EventDaoHelper helper;
    private Set<String> fields = null;

    public EventSummaryRowMapper(EventDaoHelper eventDaoHelper) {
        this.helper = eventDaoHelper;
    }

    @Override
    public EventSummary mapRow(ResultSet rs, int rowNum) throws SQLException {
        if (fields == null) {
            fields = DaoUtils.getFieldsInResultSet(rs.getMetaData());
        }
        EventSummary.Builder summaryBuilder = EventSummary.newBuilder();
        summaryBuilder.addOccurrence(helper.eventMapper(rs, true, fields));
        if (fields.contains(COLUMN_UUID)) {
            summaryBuilder.setUuid(DaoUtils.uuidFromBytes(rs
                    .getBytes(COLUMN_UUID)));
        }
        if (fields.contains(COLUMN_STATUS_ID)) {
            summaryBuilder.setStatus(EventStatus.valueOf(rs
                    .getInt(COLUMN_STATUS_ID)));
        }
        if (fields.contains(COLUMN_FIRST_SEEN)) {
            summaryBuilder.setFirstSeenTime(rs.getLong(COLUMN_FIRST_SEEN));
        }
        if (fields.contains(COLUMN_STATUS_CHANGE)) {
            summaryBuilder
                    .setStatusChangeTime(rs.getLong(COLUMN_STATUS_CHANGE));
        }
        if (fields.contains(COLUMN_LAST_SEEN)) {
            summaryBuilder.setLastSeenTime(rs.getLong(COLUMN_LAST_SEEN));
        }
        if (fields.contains(COLUMN_UPDATE_TIME)) {
            summaryBuilder.setUpdateTime(rs.getLong(COLUMN_UPDATE_TIME));
        }
        if (fields.contains(COLUMN_EVENT_COUNT)) {
            summaryBuilder.setCount(rs.getInt(COLUMN_EVENT_COUNT));
        }
        if (fields.contains(COLUMN_ACKNOWLEDGED_BY_USER_UUID)) {
            byte[] acknowledgedByUserUuid = rs
                    .getBytes(COLUMN_ACKNOWLEDGED_BY_USER_UUID);
            if (acknowledgedByUserUuid != null) {
                summaryBuilder.setAcknowledgedByUserUuid(DaoUtils
                        .uuidFromBytes(acknowledgedByUserUuid));
            }
        }
        if (fields.contains(COLUMN_ACKNOWLEDGED_BY_USER_NAME)) {
            String acknowledgedByUserName = rs.getString(COLUMN_ACKNOWLEDGED_BY_USER_NAME);
            if (acknowledgedByUserName != null) {
                summaryBuilder.setAcknowledgedByUserName(acknowledgedByUserName);
            }
        }
        if (fields.contains(COLUMN_CLEARED_BY_EVENT_UUID)) {
            byte[] clearedByEventUuid = rs
                    .getBytes(COLUMN_CLEARED_BY_EVENT_UUID);
            if (clearedByEventUuid != null) {
                summaryBuilder.setClearedByEventUuid(DaoUtils
                        .uuidFromBytes(clearedByEventUuid));
            }
        }
        if (fields.contains(COLUMN_NOTES_JSON)) {
            String notesJson = rs.getString(COLUMN_NOTES_JSON);
            if (notesJson != null) {
                try {
                    List<EventNote> notes = JsonFormat.mergeAllDelimitedFrom(
                            "[" + notesJson + "]",
                            EventNote.getDefaultInstance());
                    summaryBuilder.addAllNotes(notes);
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }
        }
        return summaryBuilder.build();
    }
}