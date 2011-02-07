/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2011, Zenoss Inc.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 *
 * For complete information please visit: http://www.zenoss.com/oss/
 */
package org.zenoss.zep.dao.impl;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.zep.Zep.EventDetailItem;
import org.zenoss.protobufs.zep.Zep.EventDetailItem.EventDetailType;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.ZepUtils;
import org.zenoss.zep.dao.EventDetailsConfigDao;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class EventDetailsConfigDaoImpl implements EventDetailsConfigDao {

    private static final Logger logger = LoggerFactory.getLogger(EventDetailsConfigDaoImpl.class);
    private Map<String, EventDetailItem> detailsItemMap;
    private Properties indexDetailsConfig;

    public void init() {
        this.detailsItemMap = loadIndexDetails();
        logger.debug("Indexed event details: " + this.detailsItemMap.values());
    }
    
    public void setIndexDetailsConfig(Properties indexDetailsConfig) {
        this.indexDetailsConfig = indexDetailsConfig;
    }

    @Override
    public Map<String, EventDetailItem> getEventDetailsIndexConfiguration() throws ZepException {
        return Collections.unmodifiableMap(this.detailsItemMap);
    }

    private Map<String,EventDetailItem> loadIndexDetails() {
        Map<String,EventDetailItem> indexDetailsMap = new HashMap<String,EventDetailItem>();
        if (indexDetailsConfig != null) {
            indexDetailsMap.putAll(loadIndexDetailsFromProperties(indexDetailsConfig));
        }

        String zenHome = System.getenv("ZENHOME");
        if (zenHome == null) {
            zenHome = System.getProperty("ZENHOME");
            if (zenHome == null) {
                zenHome = ".";
            }
        }
        File basedir = new File(zenHome, "etc/zenoss-zep/details");
        if (basedir.isDirectory()) {
            File[] propFiles = basedir.listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    return pathname.isFile() && pathname.getName().endsWith(".properties");
                }
            });
            if (propFiles != null) {
                for (File file : propFiles) {
                    Map<String,EventDetailItem> fileMap = loadIndexDetailsFromFile(file);
                    for (Map.Entry<String,EventDetailItem> entry : fileMap.entrySet()) {
                        if (!indexDetailsMap.containsKey(entry.getKey())) {
                            indexDetailsMap.put(entry.getKey(), entry.getValue());
                        }
                        else {
                            logger.warn("Duplicate index detail definition found for {} in {}",
                                    entry.getKey(), file.getName());
                        }
                    }
                }
            }
        }
        return indexDetailsMap;
    }

    private static Map<String,EventDetailItem> loadIndexDetailsFromFile(File file) {
        InputStream is = null;
        try {
            Properties props = new Properties();
            is = new BufferedInputStream(new FileInputStream(file));
            props.load(is);
            return loadIndexDetailsFromProperties(props);
        } catch (IOException e) {
            logger.warn("Failed to open properties file", e);
        } finally {
            ZepUtils.close(is);
        }
        return Collections.emptyMap();
    }

    private static Map<String,EventDetailItem> loadIndexDetailsFromProperties(Properties properties) {
        Map<String, EventDetailItem> itemDetailsMap = new HashMap<String, EventDetailItem>();
        for (Map.Entry<Object,Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            String val = (String) entry.getValue();
            if (val.isEmpty()) {
                logger.warn("Invalid property entry: {}", key);
                continue;
            }
            if (!key.endsWith(".key")) {
                continue;
            }
            if (itemDetailsMap.containsKey(val)) {
                logger.warn("Duplicate definition found for detail: {}", val);
                continue;
            }
            EventDetailItem.Builder itemBuilder = EventDetailItem.newBuilder();
            itemBuilder.setKey(val);

            String prefix = key.substring(0, key.length()-4);
            String typeStr = properties.getProperty(prefix + ".type");
            if (typeStr != null) {
                try {
                    EventDetailType type = EventDetailType.valueOf(typeStr.toUpperCase());
                    itemBuilder.setType(type);
                } catch (Exception e) {
                    logger.warn("Invalid type {} specified for {}", typeStr, prefix + ".type");
                    continue;
                }
            }
            String displayName = properties.getProperty(prefix + ".display_name");
            if (displayName != null) {
                itemBuilder.setName(displayName);
            }

            itemDetailsMap.put(val, itemBuilder.build());
        }
        return itemDetailsMap;
    }
}
