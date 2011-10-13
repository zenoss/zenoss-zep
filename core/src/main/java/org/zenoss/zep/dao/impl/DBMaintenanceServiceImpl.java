/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.util.StringUtils;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.DBMaintenanceService;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;

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

    private final DataSource ds;
    private final JdbcTemplate template;
    private DatabaseCompatibility databaseCompatibility;

    private static final Logger logger = LoggerFactory.getLogger(DBMaintenanceServiceImpl.class);
    
    // These are the most active tables in the database
    private final List<String> tablesToOptimize = Arrays.asList("event_summary", "event_summary_index_queue",
            "event_archive_index_queue", "event_trigger_signal_spool", "daemon_heartbeat");

    public DBMaintenanceServiceImpl(DataSource ds) {
        this.ds = ds;
        this.template = new JdbcTemplate(ds);
    }

    public void setDatabaseCompatibility(DatabaseCompatibility databaseCompatibility) {
        this.databaseCompatibility = databaseCompatibility;
    }

    @Override
    public void optimizeTables() throws ZepException {
        logger.debug("Optimizing tables: {}", tablesToOptimize);
        
        switch (databaseCompatibility.getDatabaseType()) {
            case MYSQL:
                this.template.update("OPTIMIZE TABLE " +
                        StringUtils.collectionToCommaDelimitedString(this.tablesToOptimize));
                break;
            
            case POSTGRESQL:
                Connection connection = null;
                Statement statement = null;
                boolean currentAutoCommit = false;
                try {
                    connection = this.ds.getConnection();
                    currentAutoCommit = connection.getAutoCommit();
                    // Running VACUUM requires not running in a transaction - to do this we have to enable autocommit.
                    connection.setAutoCommit(true);
                    statement = connection.createStatement();
                    for (String tableToOptimize : tablesToOptimize) {
                        statement.execute("VACUUM ANALYZE " + tableToOptimize);
                    }
                } catch (SQLException e) {
                    throw new SQLErrorCodeSQLExceptionTranslator(this.ds).translate("optimizeTables", "" , e);
                } finally {
                    JdbcUtils.closeStatement(statement);
                    if (connection != null) {
                        try {
                            connection.setAutoCommit(currentAutoCommit);
                        } catch (SQLException e) {
                            /* Ignored */
                            logger.debug("Failed to reset autocommit", e);
                        } finally {
                            JdbcUtils.closeConnection(connection);
                        }
                    }
                }
                break;
            
            default:
                throw new ZepException("Unsupported database type: " + databaseCompatibility.getDatabaseType());

        }

        logger.debug("Completed Optimizing tables: {}", tablesToOptimize);
    }
}
