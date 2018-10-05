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

    public ZingConfig() {}

    public ZingConfig(boolean enabled, boolean useEmulator,
                      String tenant, String source, String project, String topic,
                      String emulatorHostAndPort, String credentialsPath) {
        this.enabled = enabled;
        this.useEmulator = useEmulator;
        this.tenant = tenant;
        this.source = source;
        this.project = project;
        this.topic = topic;
        this.emulatorHostAndPort = emulatorHostAndPort;
        this.credentialsPath = credentialsPath;
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

    public String toString() {
        final StringBuffer strBuf = new StringBuffer();
        strBuf.append(" / enabled = ").append(this.enabled);
        strBuf.append(" / useEmulator = ").append(this.useEmulator);
        strBuf.append(" / emulatorUrl = ").append(this.emulatorHostAndPort);
        strBuf.append(" / tenant = ").append(this.tenant);
        strBuf.append(" / source = ").append(this.source);
        strBuf.append(" / topic = ").append(this.topic);
        strBuf.append(" / credentials path = ").append(this.credentialsPath);
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
        // FIXME when do we need creds??
        //this.credentialsPath = credentialsPath;

        return valid;
    }
}
