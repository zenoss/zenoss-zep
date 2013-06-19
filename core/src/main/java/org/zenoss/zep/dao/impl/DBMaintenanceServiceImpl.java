/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcUtils;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.ZepInstance;
import org.zenoss.zep.dao.DBMaintenanceService;
import org.zenoss.zep.dao.impl.compat.DatabaseCompatibility;
import org.zenoss.zep.dao.impl.compat.DatabaseType;
import org.zenoss.zep.dao.impl.DaoUtils;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Implementation of DBMaintenanceService for MySQL.
 */
public class DBMaintenanceServiceImpl implements DBMaintenanceService {

    private final JdbcTemplate template;

    private String hostname = "";
    private String port = "";
    private String dbname = "";
    private String username = "";
    private String password = "";

    private Boolean useExternalTool = false;
    private String externalToolOptions = "";

    private DatabaseCompatibility databaseCompatibility;

    private static final Logger logger = LoggerFactory.getLogger(DBMaintenanceServiceImpl.class);
    
    // These are the most active tables in the database
    private List<String> tablesToOptimize = new ArrayList<String>();

    public DBMaintenanceServiceImpl(DataSource ds, Properties globalConf, ZepInstance zepInstance) {
        this.template = new JdbcTemplate(ds);

        final Map<String,String> zepConfig = zepInstance.getConfig();
        this.hostname = globalConf.getProperty("zep-host", zepConfig.get("zep.jdbc.hostname"));
	this.port = globalConf.getProperty("zep-port", zepConfig.get("zep.jdbc.port"));
        this.dbname = globalConf.getProperty("zep-db", zepConfig.get("zep.jdbc.dbname"));
        this.username = globalConf.getProperty("zep-user", zepConfig.get("zep.jdbc.username"));
        this.password = globalConf.getProperty("zep-password", zepConfig.get("zep.jdbc.password"));

        this.useExternalTool = Boolean.valueOf(globalConf.getProperty("zep-optimize-use-external-tool", zepConfig.get("zep.database.optimize_use_external_tool")).trim());
        this.externalToolOptions = globalConf.getProperty("zep-optimize-external-tool-options", zepConfig.get("zep.database.optimize_external_tool_options"));

        this.tablesToOptimize.add("event_summary_index_queue");
        this.tablesToOptimize.add("event_archive_index_queue");
        this.tablesToOptimize.add("event_trigger_signal_spool");
        this.tablesToOptimize.add("daemon_heartbeat");
    }

    public void setDatabaseCompatibility(DatabaseCompatibility databaseCompatibility) {
        this.databaseCompatibility = databaseCompatibility;
    }

    @Override
    public void optimizeTables() throws ZepException {
        final DatabaseType dbType = databaseCompatibility.getDatabaseType();

        // if we want to use percona's pt-online-schema-change to avoid locking the tables due to mysql optimize...
        if(this.useExternalTool && dbType == DatabaseType.MYSQL) {
            String tableToOptimize = "event_summary";
            final String externalToolName = "pt-online-schema-change";
            logger.debug("Optimizing table: " + tableToOptimize + " via percona " + externalToolName);
            int return_code = DaoUtils.executeCommand("which " + externalToolName);
            if(return_code == 0) {
                String externalToolCommandPrefix = externalToolName + " --alter \"ENGINE=Innodb\" D=" + this.dbname + ",t=";
                String externalToolCommandSuffix = "";
                if(Integer.parseInt(System.getenv("USE_ZENDS").trim()) == 1) {
                    externalToolCommandSuffix = " --defaults-file=/opt/zends/etc/zends.cnf";
                }
                externalToolCommandSuffix += " " + this.externalToolOptions + " --alter-foreign-keys-method=drop_swap --host=" + this.hostname + " --port=" + this.port + " --user=" + this.username + " --password=" + this.password + " --execute";
                return_code = DaoUtils.executeCommand(externalToolCommandPrefix + tableToOptimize + externalToolCommandSuffix);
                if(return_code != 0) {
                    logger.error("External tool failed on: " + tableToOptimize + ". Therefore, table:" + tableToOptimize + "will not be optimized.");
                } else {
                    logger.debug("Successfully optimized table: " + tableToOptimize + "using percona " + externalToolName);
                }
            } else {
                logger.error("External tool not available. Table: " + tableToOptimize + " will not be optimized.");
            }
        } else {
            this.tablesToOptimize.add("event_summary");
        }

        logger.debug("Optimizing tables: {}", this.tablesToOptimize);
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
