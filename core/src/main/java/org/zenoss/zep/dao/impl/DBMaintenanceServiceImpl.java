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
import org.zenoss.protobufs.model.Model.ModelElementType;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.zep.EventPublisher;
import org.zenoss.zep.UUIDGenerator;
import org.zenoss.zep.ZepConstants;
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
import java.util.concurrent.TimeUnit;

class ElapsedTime {

    private long startTime = 0;
    private long endTime = 0;

    ElapsedTime() {
        setStartTime();
    }

    public void setStartTime() {
        this.startTime = System.currentTimeMillis();
        endTime = 0;
    }

    public void setEndTime() {
        this.endTime = System.currentTimeMillis();
    }

    public long getElapsedTime() {
        return this.endTime > this.startTime ? this.endTime - this.startTime : 0;
    }

    public static String formatElapsed(final long elapsedMillis) {
        final long hr  = TimeUnit.MILLISECONDS.toHours(elapsedMillis);
        final long min = TimeUnit.MILLISECONDS.toMinutes(elapsedMillis
                            - TimeUnit.HOURS.toMillis(hr));
        final long sec = TimeUnit.MILLISECONDS.toSeconds(elapsedMillis
                            - TimeUnit.HOURS.toMillis(hr)
                            - TimeUnit.MINUTES.toMillis(min));
        final long ms  = TimeUnit.MILLISECONDS.toMillis(elapsedMillis
                            - TimeUnit.HOURS.toMillis(hr)
                            - TimeUnit.MINUTES.toMillis(min)
                            - TimeUnit.SECONDS.toMillis(sec));
        return String.format("%02dh:%02dm:%02d.%03ds", hr, min, sec, ms);
    }

    public String elapsedTime() {
        return formatElapsed(getElapsedTime());
    }
}

class DefaultValue {
    public static <T> T defaultValue(T value, T aDefaultValue) {
        return value != null ? value : aDefaultValue;
    }
}

/**
 * Implementation of DBMaintenanceService for MySQL.
 */
public class DBMaintenanceServiceImpl implements DBMaintenanceService {

    private static final String MONITOR_ZEP = "localhost";
    private static final String DAEMON_ZEP  = "zeneventserver";
    private static final String STATUS_ZEP  = "/Status/ZEP";
    private static final String ELAPSED_WARN = "zep.database.optimize_elapsed_warn_threshold_seconds";

    private final JdbcTemplate template;
    private final String useExternalToolPath;
    private EventPublisher eventPublisher;
    private UUIDGenerator uuidGenerator;

    private String hostname = "";
    private String port = "";
    private String dbname = "";
    private String username = "";
    private String password = "";

    private Boolean useExternalTool = true;
    private String externalToolOptions = "";
    private Integer elapsedWarnThresholdSeconds = 120;  // 0: disabled, <: send INFO, >=: send WARNING
    private final ElapsedTime eventSummaryOptimizationTime = new ElapsedTime();

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
        this.useExternalToolPath = globalConf.getProperty("zep-optimize-external-tool-path", zepConfig.get("zep.database.optimize_external_tool_path"));
        this.externalToolOptions = globalConf.getProperty("zep-optimize-external-tool-options", zepConfig.get("zep.database.optimize_external_tool_options"));
        this.elapsedWarnThresholdSeconds = Integer.valueOf(
                DefaultValue.defaultValue(
                    globalConf.getProperty("zep-optimize-elapsed-warn-threshold-seconds",
                        zepConfig.get(ELAPSED_WARN)),
                    String.valueOf(this.elapsedWarnThresholdSeconds)));

        this.tablesToOptimize.add("event_trigger_signal_spool");
        this.tablesToOptimize.add("daemon_heartbeat");
    }

    public void setDatabaseCompatibility(DatabaseCompatibility databaseCompatibility) {
        this.databaseCompatibility = databaseCompatibility;
    }

    public void setEventPublisher(EventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    public void setUuidGenerator(UUIDGenerator uuidGenerator) {
        this.uuidGenerator = uuidGenerator;
    }

    private Event createElapsedEvent(EventSeverity severity, String summary, String message, String fingerprint) {
        // taken and modified from createHeartbeatEvent()
        final long createdTime = System.currentTimeMillis();
        final Event.Builder event = Event.newBuilder();
        event.setUuid(this.uuidGenerator.generate().toString());
        event.setCreatedTime(createdTime);
        final EventActor.Builder actor = event.getActorBuilder();
        actor.setElementIdentifier(MONITOR_ZEP).setElementTypeId(ModelElementType.DEVICE);
        actor.setElementSubIdentifier(DAEMON_ZEP).setElementSubTypeId(ModelElementType.COMPONENT);
        event.setMonitor(MONITOR_ZEP);
        event.setAgent(DAEMON_ZEP);
        // Per old behavior - alerting rules typically are configured to only fire
        // for devices in production. These events don't have a true "device" with
        // a production state a lot of the time, and so we have to set this manually.
        event.addDetailsBuilder().setName(ZepConstants.DETAIL_DEVICE_PRODUCTION_STATE)
                .addValue(Integer.toString(ZepConstants.PRODUCTION_STATE_PRODUCTION));
        event.setSeverity(severity);
        event.setSummary(summary);
        event.setEventClass(STATUS_ZEP);
        event.setMessage(message);
        event.setFingerprint(fingerprint);

        // set eventKey - CLEAR only cares about these fields: device, component, eventKey, and eventClass
        if (severity == EventSeverity.SEVERITY_INFO) {
            event.setEventKey("INFO");
        } else {
            event.setEventKey("ALERT");
        }

        return event.build();
    }

    private void SendOptimizationTimeEvent(ElapsedTime elapsedTime, String tableToOptimize, String toolName) throws ZepException {
        if (this.elapsedWarnThresholdSeconds <= 0) {
            return;
        }

        long elapsedTimeMillis = elapsedTime.getElapsedTime();
        String summary = "Optimizaton of " + tableToOptimize;
        if (toolName.length() > 0) {
            summary += " (via " + toolName + ")";
        }
        String fingerprintSuffix = "optimize " + tableToOptimize;
        String fingerprint = "ZEP INFO:" + fingerprintSuffix;
        String message = summary;
        final Event event = createElapsedEvent(EventSeverity.SEVERITY_INFO,
                summary + " took " + ElapsedTime.formatElapsed(elapsedTimeMillis),
                message, fingerprint);
        logger.debug("Publishing optimization elapsed time INFO event: {}", event);
        eventPublisher.publishEvent(event);

        fingerprint = "ZEP ALERT:" + fingerprintSuffix;
        if (toolName.length() <= 0 && elapsedTimeMillis >= (this.elapsedWarnThresholdSeconds * 1000)) {
            summary += " exceeded threshold of " + String.valueOf(elapsedWarnThresholdSeconds) + " seconds";
            logger.warn(summary);

            message = summary + ".  This exceeds the warn threshold of "
                + String.valueOf(elapsedWarnThresholdSeconds) + " seconds "
                + "(configurable in etc/zeneventserver.conf with the setting " + ELAPSED_WARN + ")."
                + "  This may be indicative of a performance issue with the zenoss_zep database server."
                + "  " // wish we could insert newlines into message here to separate into paragraphs
                + "  Analyzing the performance of your database optimize calls may be needed."
                + "  If the process is running as expected you can increase the warn threshold to a reasonable setting for your environment."
            ;

            final Event eventWarn = createElapsedEvent(EventSeverity.SEVERITY_WARNING, summary, message, fingerprint);
            logger.debug("Publishing optimization elapsed time ALERT event: {}", eventWarn);
            eventPublisher.publishEvent(eventWarn);
        } else {
            logger.info(summary);

            final Event eventClear = createElapsedEvent(EventSeverity.SEVERITY_CLEAR, summary + " - CLEAR", message, fingerprint);
            logger.debug("Publishing optimization elapsed time CLEAR event: {}", eventClear);
            eventPublisher.publishEvent(eventClear);
        }
    }

    @Override
    public void optimizeTables() throws ZepException {
        final DatabaseType dbType = databaseCompatibility.getDatabaseType();

        final String externalToolName = this.useExternalToolPath + "/pt-online-schema-change";
        final String tableToOptimize = "event_summary";
        // if we want to use percona's pt-online-schema-change to avoid locking the tables due to mysql optimize...
        //checks if external tool is available
        if (this.useExternalTool && dbType == DatabaseType.MYSQL && DaoUtils.executeCommand("ls " + externalToolName) == 0) {
            logger.info("Validating state of event_summary");
            this.validateEventSummaryState();
            logger.debug("Optimizing table: " + tableToOptimize + " via percona " + externalToolName);
            eventSummaryOptimizationTime.setStartTime();

            String externalToolCommandPrefix = externalToolName + " --alter \"ENGINE=Innodb\" D=" + this.dbname + ",t=";
            String externalToolCommandSuffix = "";
            if (System.getenv("USE_ZENDS") != null && Integer.parseInt(System.getenv("USE_ZENDS").trim()) == 1) {
                externalToolCommandSuffix = " --defaults-file=/opt/zends/etc/zends.cnf";
            }
            externalToolCommandSuffix += " " + this.externalToolOptions + " --alter-foreign-keys-method=drop_swap --host=" + this.hostname + " --port=" + this.port + " --user=" + this.username + " --password=" + this.password + " --execute";
            int return_code = DaoUtils.executeCommand(externalToolCommandPrefix + tableToOptimize + externalToolCommandSuffix);
            if (return_code != 0) {
                logger.error("External tool failed on: " + tableToOptimize + ". Therefore, table:" + tableToOptimize + "will not be optimized.");
            } else {
                logger.debug("Successfully optimized table: " + tableToOptimize + "using percona " + externalToolName);
            }

            eventSummaryOptimizationTime.setEndTime();
            SendOptimizationTimeEvent(eventSummaryOptimizationTime, tableToOptimize, "percona");

            if (this.tablesToOptimize.contains(tableToOptimize)) {
                this.tablesToOptimize.remove(tableToOptimize);
            }
        } else {
            if (this.useExternalTool) {
                logger.warn("External tool not available. Table: " + tableToOptimize + " optimization may be slow.");
            }
            if (!this.tablesToOptimize.contains(tableToOptimize)) {
                this.tablesToOptimize.add(tableToOptimize);
            }
        }

        eventSummaryOptimizationTime.setStartTime(); // init so elapsedTime() == 0

        try {
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
                            if (tableToOptimize == "event_summary") {
                                eventSummaryOptimizationTime.setStartTime();
                            }
                            statement.execute(sql);
                            if (tableToOptimize == "event_summary") {
                                eventSummaryOptimizationTime.setEndTime();
                            }
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
        } finally {
            logger.info("Validating state of event_summary");
            this.validateEventSummaryState();
        }

        if (eventSummaryOptimizationTime.getElapsedTime() > 0) {
            SendOptimizationTimeEvent(eventSummaryOptimizationTime, "event_summary", "");
        }

        logger.debug("Completed Optimizing tables: {}", tablesToOptimize);
    }

    @Override
    public void validateEventSummaryState() throws ZepException {

        // pt-online-schema-change failures can lead to triggers that point to a missing table: ZEN-7474
        this.template.update("DROP TRIGGER IF EXISTS pt_osc_zenoss_zep_event_summary_upd");
        this.template.update("DROP TRIGGER IF EXISTS pt_osc_zenoss_zep_event_summary_ins");
        this.template.update("DROP TRIGGER IF EXISTS pt_osc_zenoss_zep_event_summary_del");
        this.template.update("DROP TABLE IF EXISTS _event_summary_new");
    }


}
