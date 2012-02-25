/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcUtils;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.DBMaintenanceService;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.DatabaseType;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Implementation of DBMaintenanceService for MySQL.
 */
public class DBMaintenanceServiceImpl implements DBMaintenanceService {

    private final JdbcTemplate template;
    private DatabaseCompatibility databaseCompatibility;

    private static final Logger logger = LoggerFactory.getLogger(DBMaintenanceServiceImpl.class);
    
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
        logger.debug("Optimizing tables: {}", tablesToOptimize);

        final DatabaseType dbType = databaseCompatibility.getDatabaseType();

        this.template.execute(new ConnectionCallback<Object>() {
            @Override
            public Object doInConnection(Connection con) throws SQLException, DataAccessException {
                Boolean currentAutoCommit = null;
                Statement statement = null;
                try {
                    currentAutoCommit = con.getAutoCommit();
                    con.setAutoCommit(true);
                    statement = con.createStatement();
                    for (String tableToOptimize : tablesToOptimize) {
                        logger.debug("Optimizing table: {}", tableToOptimize);
                        final String sql;
                        switch (dbType) {
                            case MYSQL:
                                sql = "OPTIMIZE TABLE " + tableToOptimize;
                                break;
                            case POSTGRESQL:
                                sql = "VACUUM ANALYZE " + tableToOptimize;
                                break;
                            default:
                                throw new IllegalStateException("Unsupported database type: " + dbType);
                        }
                        statement.execute(sql);
                        logger.debug("Completed optimizing table: {}", tableToOptimize);
                    }
                } finally {
                    JdbcUtils.closeStatement(statement);
                    if (currentAutoCommit != null) {
                        con.setAutoCommit(currentAutoCommit);
                    }
                }
                return null;
            }
        });

        logger.debug("Completed Optimizing tables: {}", tablesToOptimize);
    }
}
