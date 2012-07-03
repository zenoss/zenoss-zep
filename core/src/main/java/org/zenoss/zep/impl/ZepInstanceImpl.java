/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.zep.UUIDGenerator;
import org.zenoss.zep.ZepInstance;
import org.zenoss.zep.ZepUtils;

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

public class ZepInstanceImpl implements ZepInstance {

    private static final Logger logger = LoggerFactory
            .getLogger(ZepInstanceImpl.class.getName());
    private final String instanceId;
    private final Map<String, String> config;
    private final UUIDGenerator uuidGenerator;

    public ZepInstanceImpl(Properties config, UUIDGenerator uuidGenerator) throws IOException {
        this.uuidGenerator = uuidGenerator;
        this.instanceId = loadInstanceId();
        this.config = createConfig(config);
    }

    private Map<String, String> createConfig(Properties config) {
        Map<String, String> cfg = new HashMap<String, String>(config.size());
        for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
            Object key = entry.getKey();
            Object val = entry.getValue();
            if (key instanceof String && val instanceof String) {
                String strKey = (String) key;
                String strVal = (String) val;
                cfg.put(strKey, strVal);
            }
        }
        for (Map.Entry<Object, Object> entry : config.entrySet()) {
            Object key = entry.getKey();
            Object val = entry.getValue();
            if (key instanceof String && val instanceof String) {
                String strKey = (String) key;
                String strVal = (String) val;
                if (!cfg.containsKey(strKey)) {
                    cfg.put(strKey, strVal);
                }
            }
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
            id = this.uuidGenerator.generate().toString();
        } else {
            File f = new File(zenHome, "etc/zeneventserver/instance.properties");
            Properties props = loadProperties(f);
            id = props.getProperty("id");

            // Persist ID to disk
            if (id == null || !isValidUuid(id)) {
                id = this.uuidGenerator.generate().toString();
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
            if (!f.getParentFile().isDirectory() && !f.getParentFile().mkdirs()) {
                throw new IOException("Failed to create parent directory: " +
                        f.getParentFile().getAbsolutePath());
            }
            bos = new BufferedOutputStream(new FileOutputStream(f));
            properties.store(bos, "ZEP Instance ID. Do not modify");
        } finally {
            ZepUtils.close(bos);
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
