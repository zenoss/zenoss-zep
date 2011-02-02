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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.zep.ConfigConstants;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.ConfigDao;
import org.zenoss.zep.events.ConfigUpdatedEvent;

public class ConfigDaoImpl implements ConfigDao, ApplicationEventPublisherAware {

    private static final Logger logger = LoggerFactory
            .getLogger(ConfigDao.class);
    private final SimpleJdbcTemplate template;
    private ApplicationEventPublisher applicationEventPublisher;
    private static final int MAX_PARTITIONS = 1000;
    private final int maxArchiveDays;
    private final int maxOccurrenceDays;

    public ConfigDaoImpl(DataSource ds, PartitionConfig partitionConfig) {
        this.template = new SimpleJdbcTemplate(ds);
        
        this.maxArchiveDays = calculateMaximumDays(partitionConfig
                .getConfig(TABLE_EVENT_ARCHIVE));
        logger.info("Maximum archive days: {}", maxArchiveDays);
        
        this.maxOccurrenceDays = calculateMaximumDays(partitionConfig
                .getConfig(TABLE_EVENT));
        logger.info("Maximum occurrence days: {}", maxOccurrenceDays);
    }

    private static int calculateMaximumDays(PartitionTableConfig config) {
        long partitionRange = config.getPartitionUnit().toMinutes(
                config.getPartitionDuration())
                * MAX_PARTITIONS;
        return (int) TimeUnit.DAYS.convert(partitionRange, TimeUnit.MINUTES);
    }

    @Override
    public void setApplicationEventPublisher(
            ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Override
    @Transactional(readOnly = true)
    public Map<String, String> getConfig() throws ZepException {
        try {
            final Map<String, String> cfg = new HashMap<String, String>();
            template.getJdbcOperations().query("SELECT * FROM config",
                    new RowCallbackHandler() {
                        @Override
                        public void processRow(ResultSet rs)
                                throws SQLException {
                            cfg.put(rs.getString("config_name"),
                                    rs.getString("config_value"));
                        }
                    });
            return cfg;
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @Transactional(readOnly = true)
    public String getConfigValue(String name) throws ZepException {
        try {
            List<String> result = template.query(
                    "SELECT config_value FROM config WHERE config_name=?",
                    new RowMapper<String>() {
                        @Override
                        public String mapRow(ResultSet rs, int rowNum)
                                throws SQLException {
                            return rs.getString("config_value");
                        }
                    }, name);
            return (result.isEmpty()) ? null : result.get(0);
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @Transactional
    public int removeConfigValue(String name) throws ZepException {
        try {
            final int changed = this.template.update(
                    "DELETE FROM config WHERE config_name=?", name);
            if (changed > 0) {
                this.applicationEventPublisher
                        .publishEvent(new ConfigUpdatedEvent(this, name));
            }
            return changed;
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @Transactional
    public void setConfig(Map<String, String> config) throws ZepException {
        try {
            validateConfig(config);
            this.template.update("DELETE FROM config");
            List<Object[]> batchArgs = new ArrayList<Object[]>(config.size());
            for (Map.Entry<String, String> entry : config.entrySet()) {
                batchArgs
                        .add(new Object[] { entry.getKey(), entry.getValue() });
            }
            this.template
                    .batchUpdate(
                            "INSERT INTO config (config_name,config_value) VALUES(?,?)",
                            batchArgs);
            this.applicationEventPublisher.publishEvent(new ConfigUpdatedEvent(
                    this, config));
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    @Override
    @Transactional
    public void setConfigValue(String name, String value) throws ZepException {
        try {
            validateConfigEntry(name, value);
            this.template
                    .update("INSERT INTO config (config_name,config_value) VALUES(?,?) "
                            + "ON DUPLICATE KEY UPDATE config_value=VALUES(config_value)",
                            name, value);
            this.applicationEventPublisher.publishEvent(new ConfigUpdatedEvent(
                    this, name, value));
        } catch (DataAccessException e) {
            throw new ZepException(e);
        }
    }

    private void validateConfig(Map<String, String> config) throws ZepException {
        for (Map.Entry<String, String> entry : config.entrySet()) {
            validateConfigEntry(entry.getKey(), entry.getValue());
        }
    }

    private void validateConfigEntry(String name, String value)
            throws ZepException {
        try {
            if (ConfigConstants.CONFIG_EVENT_AGE_DISABLE_SEVERITY.equals(name)) {
                EventSeverity severity = EventSeverity.valueOf(value);
                if (severity == null) {
                    throw new IllegalArgumentException();
                }
            } else if (ConfigConstants.CONFIG_EVENT_AGE_INTERVAL_MINUTES
                    .equals(name)) {
                Integer.valueOf(value);
                /* No restrictions on value */
            } else if (ConfigConstants.CONFIG_EVENT_ARCHIVE_PURGE_INTERVAL_DAYS
                    .equals(name)) {
                int purgeIntervalDays = Integer.valueOf(value);
                if (purgeIntervalDays < 1 || purgeIntervalDays > maxArchiveDays) {
                    throw new IllegalArgumentException();
                }
            } else if (ConfigConstants.CONFIG_EVENT_OCCURRENCE_PURGE_INTERVAL_DAYS
                    .equals(name)) {
                int purgeOccurrenceDays = Integer.valueOf(value);
                if (purgeOccurrenceDays < 1
                        || purgeOccurrenceDays > maxOccurrenceDays) {
                    throw new IllegalArgumentException();
                }
            } else if (ConfigConstants.CONFIG_EVENT_ARCHIVE_INTERVAL_DAYS
                    .equals(name)) {
                int archiveDays = Integer.valueOf(value);
                if (archiveDays < 1
                        || archiveDays > ConfigConstants.MAX_EVENT_ARCHIVE_INTERVAL_DAYS)
                    throw new IllegalArgumentException();
            }
        } catch (RuntimeException e) {
            throw new ZepException(String.format(
                    "Invalid value specified for %s: %s", name, value));
        }
    }
}
