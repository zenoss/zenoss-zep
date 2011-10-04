/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.StringUtils;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.DBMaintenanceService;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.List;

/**
 * Implementation of DBMaintenanceService for MySQL.
 */
public class DBMaintenanceServiceImpl implements DBMaintenanceService {
    
    private final JdbcTemplate template;
    private DatabaseCompatibility databaseCompatibility;
    
    // These are the most active tables in the database
    private final List<String> tablesToOptimize = Arrays.asList("event_summary", "event_summary_index_queue",
            "event_archive_index_queue", "event_trigger_signal_spool", "daemon_heartbeat");

    public DBMaintenanceServiceImpl(DataSource ds) {
        this.template = new JdbcTemplate(ds);
    }

    public void setDatabaseCompatibility(DatabaseCompatibility databaseCompatibility) {
        this.databaseCompatibility = databaseCompatibility;
    }

    @Override
    public void optimizeTables() throws ZepException {
        final String sql;
        switch (databaseCompatibility.getDatabaseType()) {
            case MYSQL:
                sql = "OPTIMIZE TABLE " + StringUtils.collectionToCommaDelimitedString(tablesToOptimize);
                break;
            case POSTGRESQL:
                sql = "VACUUM ANALYZE TABLE " + StringUtils.collectionToCommaDelimitedString(tablesToOptimize);
                break;
            default:
                throw new ZepException("Unsupported database type: " + databaseCompatibility.getDatabaseType());
        }
        this.template.update(sql);
    }
}
