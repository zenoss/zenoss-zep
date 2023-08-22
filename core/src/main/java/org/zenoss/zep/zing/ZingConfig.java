/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2018, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.zing;

import org.python.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;


public class ZingConfig {
    
    private static final Logger logger = LoggerFactory.getLogger(ZingConfig.class);

    public boolean enabled = false;

    public boolean useEmulator = false;

    public boolean usePubsubLite = false;

    public String tenant = "";

    public String source = "";

    public String project = "";

    public String topic = "";

    public long pubsubLiteProjectNumber = 0;

    public String pubsubLiteLocation = "";

    public String emulatorHostAndPort = "";

    public String credentialsPath = "";

    public String minimumSeverity = "";

    public Integer maxPubsubMessageSize = 0;

    public Integer maxEventFieldLength = 0;

    public ZingConfig() {}

    public ZingConfig(boolean enabled, boolean useEmulator, boolean usePubsubLite,
                      String tenant, String source, String project, String topic,
                      long pubsubLiteProjectNumber, String pubsubLiteLocation,
                      String emulatorHostAndPort, String credentialsPath, String minimumSeverity,
                      Integer maxPubsubMessageSize, Integer maxEventFieldLength) {
        this.enabled = enabled;
        this.useEmulator = useEmulator;
        this.usePubsubLite = usePubsubLite;
        this.tenant = tenant;
        this.source = source;
        this.project = project;
        this.topic = topic;
        this.pubsubLiteProjectNumber = pubsubLiteProjectNumber;
        this.pubsubLiteLocation = pubsubLiteLocation;
        this.emulatorHostAndPort = emulatorHostAndPort;
        this.credentialsPath = credentialsPath;
        this.minimumSeverity = minimumSeverity;
        this.maxPubsubMessageSize = maxPubsubMessageSize;
        this.maxEventFieldLength = maxEventFieldLength;

        this.setDefaults();
    }

    public void setDefaults() {
        // set default values for pubsub lite options based upon the project
        if (this.pubsubLiteProjectNumber == 0) {
            switch (this.project) {
                case "zing-dev-197522":     this.pubsubLiteProjectNumber = 303933868810L; break;
                case "zing-testing-200615": this.pubsubLiteProjectNumber = 744199835707L; break;
                case "zing-preview":        this.pubsubLiteProjectNumber = 282430405229L; break;
                case "zing-perf":           this.pubsubLiteProjectNumber = 1091318775822L; break;
                case "zcloud-emea":         this.pubsubLiteProjectNumber = 135493714097L; break;
                case "zcloud-prod":         this.pubsubLiteProjectNumber = 29121105001L; break;
                case "zcloud-prod2":        this.pubsubLiteProjectNumber = 204978327501L; break;
                case "zcloud-prod3":        this.pubsubLiteProjectNumber = 795557937070L; break;
            }
        }

        if (Strings.isNullOrEmpty(this.pubsubLiteLocation)) {
            switch (this.project) {
                case "zing-dev-197522":     this.pubsubLiteLocation = "us-central1-c"; break;
                case "zing-testing-200615": this.pubsubLiteLocation = "us-central1-c"; break;
                case "zing-preview":        this.pubsubLiteLocation = "us-central1-c"; break;
                case "zing-perf":           this.pubsubLiteLocation = "us-central1-c"; break;
                case "zcloud-emea":         this.pubsubLiteLocation = "europe-west3-a"; break;
                case "zcloud-prod":         this.pubsubLiteLocation = "us-central1-c"; break;
                case "zcloud-prod2":        this.pubsubLiteLocation = "us-west4-a"; break;
                case "zcloud-prod3":        this.pubsubLiteLocation = "australia-southeast1-a"; break;
            }
        }
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

    public void setUsePubsubLite(boolean usePubsubLite) {
        this.usePubsubLite = usePubsubLite;
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

    public void setPubsubLiteProjectNumber(long pubsubLiteProjectNumber) {
        this.pubsubLiteProjectNumber = pubsubLiteProjectNumber;
    }

    public void setPubsubLiteLocation(String pubsubLiteLocation) {
        this.pubsubLiteLocation = pubsubLiteLocation;
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
        strBuf.append(" / usePubsubLite = ").append(this.usePubsubLite);
        strBuf.append(" / emulatorUrl = ").append(this.emulatorHostAndPort);
        strBuf.append(" / tenant = ").append(this.tenant);
        strBuf.append(" / source = ").append(this.source);
        strBuf.append(" / project = ").append(this.project);
        strBuf.append(" / topic = ").append(this.topic);
        strBuf.append(" / pubsubLiteProjectNumber = ").append(this.pubsubLiteProjectNumber);
        strBuf.append(" / pubsubLiteLocation = ").append(this.pubsubLiteLocation);
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
        if (valid && this.usePubsubLite &&
            (this.pubsubLiteProjectNumber == 0 || this.pubsubLiteLocation.isEmpty()) ){
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
