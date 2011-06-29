/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;

import org.springframework.jdbc.core.RowMapper;
import org.zenoss.protobufs.JsonFormat;
import org.zenoss.protobufs.zep.Zep.EventAuditLog;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.zenoss.zep.dao.impl.EventConstants.*;

public class EventSummaryRowMapper implements RowMapper<EventSummary> {
    private final EventDaoHelper helper;

    public EventSummaryRowMapper(EventDaoHelper eventDaoHelper) {
        this.helper = eventDaoHelper;
    }

    @Override
    public EventSummary mapRow(ResultSet rs, int rowNum) throws SQLException {
        final EventSummary.Builder summaryBuilder = EventSummary.newBuilder();
        summaryBuilder.addOccurrence(helper.eventMapper(rs));
        summaryBuilder.setUuid(DaoUtils.uuidFromBytes(rs.getBytes(COLUMN_UUID)));
        summaryBuilder.setStatus(EventStatus.valueOf(rs.getInt(COLUMN_STATUS_ID)));
        summaryBuilder.setFirstSeenTime(rs.getLong(COLUMN_FIRST_SEEN));
        summaryBuilder.setStatusChangeTime(rs.getLong(COLUMN_STATUS_CHANGE));
        summaryBuilder.setLastSeenTime(rs.getLong(COLUMN_LAST_SEEN));
        summaryBuilder.setUpdateTime(rs.getLong(COLUMN_UPDATE_TIME));
        summaryBuilder.setCount(rs.getInt(COLUMN_EVENT_COUNT));
        byte[] currentUserUuid = rs.getBytes(COLUMN_CURRENT_USER_UUID);
        if (currentUserUuid != null) {
            summaryBuilder.setCurrentUserUuid(DaoUtils.uuidFromBytes(currentUserUuid));
        }
        String currentUserName = rs.getString(COLUMN_CURRENT_USER_NAME);
        if (currentUserName != null) {
            summaryBuilder.setCurrentUserName(currentUserName);
        }
        byte[] clearedByEventUuid = rs.getBytes(COLUMN_CLEARED_BY_EVENT_UUID);
        if (clearedByEventUuid != null) {
            summaryBuilder.setClearedByEventUuid(DaoUtils.uuidFromBytes(clearedByEventUuid));
        }
        String notesJson = rs.getString(COLUMN_NOTES_JSON);
        if (notesJson != null) {
            try {
                List<EventNote> notes = JsonFormat.mergeAllDelimitedFrom("[" + notesJson + "]",
                        EventNote.getDefaultInstance());
                summaryBuilder.addAllNotes(notes);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
        String auditJson = rs.getString(COLUMN_AUDIT_JSON);
        if (auditJson != null) {
            try {
                List<EventAuditLog> auditLog = JsonFormat.mergeAllDelimitedFrom("[" + auditJson + "]",
                        EventAuditLog.getDefaultInstance());
                summaryBuilder.addAllAuditLog(auditLog);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }

        return summaryBuilder.build();
    }
}
