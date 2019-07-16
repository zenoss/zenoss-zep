/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2018, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.zing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;


public class ZingConfig {
    
    private static final Logger logger = LoggerFactory.getLogger(ZingConfig.class);

    public boolean enabled = false;

    public boolean useEmulator = false;

    public String tenant = "";

    public String source = "";

    public String project = "";

    public String topic = "";

    public String emulatorHostAndPort = "";

    public String credentialsPath = "";

    public String minimumSeverity = "";

    public Integer maxPubsubMessageSize = 0;

    public Integer maxEventFieldLength = 0;

    public ZingConfig() {}

    public ZingConfig(boolean enabled, boolean useEmulator,
                      String tenant, String source, String project, String topic,
                      String emulatorHostAndPort, String credentialsPath, String minimumSeverity,
                      Integer maxPubsubMessageSize, Integer maxEventFieldLength) {
        this.enabled = enabled;
        this.useEmulator = useEmulator;
        this.tenant = tenant;
        this.source = source;
        this.project = project;
        this.topic = topic;
        this.emulatorHostAndPort = emulatorHostAndPort;
        this.credentialsPath = credentialsPath;
        this.minimumSeverity = minimumSeverity;
        this.maxPubsubMessageSize = maxPubsubMessageSize;
        this.maxEventFieldLength = maxEventFieldLength;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean forwardEvents() {
        return this.enabled;
    }

    public void setUseEmulator(boolean useEmulator) {
        this.useEmulator = useEmulator;
    }

    public void setTenant(String tnt) {
        this.tenant = tnt;
    }

    public void setSource(String src) {
        this.source = src;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setEmulatorHostAndPort(String hostAndPort) {
        this.emulatorHostAndPort = hostAndPort;
    }

    public void setCredentialsPath(String creds) {
        this.credentialsPath = creds;
    }

    public void setMinimumSeverity(String severity) { this.minimumSeverity = severity; }

    public void setMaxPubsubMessageSize(Integer size) {
        this.maxPubsubMessageSize = size;
    }

    public void setMaxEventFieldLength(Integer lgth) {
        this.maxEventFieldLength = lgth;
    }

    public String toString() {
        final StringBuffer strBuf = new StringBuffer();
        strBuf.append(" / enabled = ").append(this.enabled);
        strBuf.append(" / useEmulator = ").append(this.useEmulator);
        strBuf.append(" / emulatorUrl = ").append(this.emulatorHostAndPort);
        strBuf.append(" / tenant = ").append(this.tenant);
        strBuf.append(" / source = ").append(this.source);
        strBuf.append(" / topic = ").append(this.topic);
        strBuf.append(" / credentials path = ").append(this.credentialsPath);
        strBuf.append(" / min severity = ").append(this.minimumSeverity);
        strBuf.append(" / max pubsub message size = ").append(this.maxPubsubMessageSize);
        strBuf.append(" / max event field length = ").append(this.maxEventFieldLength);
        return strBuf.toString();
    }

    public boolean validate() {
        boolean valid = true;
        if ( this.tenant.isEmpty() || this.source.isEmpty() ||
             this.project.isEmpty() || this.topic.isEmpty() ) {
            valid = false;
        }
        if (valid && this.useEmulator && this.emulatorHostAndPort.isEmpty()) {
            valid = false;
        }
        if (valid && !this.credentialsPath.isEmpty()) {
            File f = new File(this.credentialsPath);
            if ( !(f.isFile() && f.canRead()) ) {
                valid = false;
            }
        }
        return valid;
    }
}
