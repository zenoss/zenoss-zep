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
package org.zenoss.zep.impl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.zep.ZepInstance;
import org.zenoss.zep.ZepUtils;

public class ZepInstanceImpl implements ZepInstance {

    private static final Logger logger = LoggerFactory
            .getLogger(ZepInstanceImpl.class.getName());
    private final String instanceId;
    private final Map<String, String> config;

    public ZepInstanceImpl(Properties config) throws IOException {
        this.instanceId = loadInstanceId();
        this.config = createConfig(config);
    }

    private Map<String, String> createConfig(Properties config) {
        Map<String, String> cfg = new HashMap<String, String>(config.size());
        for (Map.Entry<Object, Object> entry : config.entrySet()) {
            String key = (String) entry.getKey();
            String val = (String) entry.getValue();
            // Override definition with system property value
            val = System.getProperty(key, val);
            cfg.put(key, val);
        }
        return cfg;
    }

    private String loadInstanceId() throws IOException {
        String id;
        String zenHome = System.getenv("ZENHOME");
        if (zenHome == null) {
            zenHome = System.getProperty("ZENHOME");
        }
        if (zenHome == null) {
            logger.warn("ZENHOME not specified. Not persisting ZEP instance id.");
            id = new UUID(0, 0).toString();
        } else {
            File f = new File(zenHome, "etc/zenoss-zep-instance.properties");
            Properties props = loadProperties(f);
            id = props.getProperty("id");

            // Persist ID to disk
            if (id == null || !isValidUuid(id)) {
                id = UUID.randomUUID().toString();
                props.put("id", id);
                saveProperties(props, f);
            }
        }
        return id;
    }

    private static boolean isValidUuid(String uuid) {
        boolean valid = false;
        try {
            UUID.fromString(uuid);
            valid = true;
        } catch (Exception e) {
            logger.warn("Invalid UUID: {}", uuid);
        }
        return valid;
    }

    private Properties loadProperties(File f) throws IOException {
        Properties props = new Properties();
        if (f.isFile()) {
            BufferedInputStream bis = null;
            try {
                bis = new BufferedInputStream(new FileInputStream(f));
                props.load(bis);
                return props;
            } finally {
                if (bis != null) {
                    ZepUtils.close(bis);
                }
            }
        }
        return props;
    }

    private void saveProperties(Properties properties, File f)
            throws IOException {
        BufferedOutputStream bos = null;
        try {
            bos = new BufferedOutputStream(new FileOutputStream(f));
            properties.store(bos, "ZEP Instance ID. Do not modify");
        } finally {
            if (bos != null) {
                ZepUtils.close(bos);
            }
        }
    }

    @Override
    public String getId() {
        return this.instanceId;
    }

    @Override
    public Map<String, String> getConfig() {
        return Collections.unmodifiableMap(this.config);
    }

    @Override
    public String toString() {
        return String.format("ZepInstance[id=%s]", this.instanceId);
    }

}
