package org.zenoss.zep.dao.impl;

import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Maps rows from the event archive to EventSummary objects.
 * <p/>
 * Differs from EventSummaryRowMapper in that it adds an "is_archive" detail to the EventSummary
 */
public class EventArchiveRowMapper extends EventSummaryRowMapper {

    private final EventDaoHelper helper;

    public EventArchiveRowMapper(EventDaoHelper eventDaoHelper, DatabaseCompatibility databaseCompatibility) {
        super(eventDaoHelper, databaseCompatibility);
        this.helper = eventDaoHelper;
    }

    @Override
    protected Event mapEvent(ResultSet rs) throws SQLException {
        return helper.eventMapper(rs, true);
    }
}
