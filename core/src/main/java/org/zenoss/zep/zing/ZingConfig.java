
package org.zenoss.zep.zing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZingConfig {
    
    private static final Logger logger = LoggerFactory.getLogger(ZingConfig.class);

    private boolean enabled = false;

    private boolean useEmulator = false;

    private String tenant = "";

    private String source = "";

    private String topic = "";

    private String emulatorHostAndPort = "";

    private String credentialsPath = "";

    public ZingConfig() {}

    public ZingConfig(boolean enabled, boolean useEmulator,
                      String tenant, String source, String topic,
                      String emulatorHostAndPort, String credentialsPath) {
        this.enabled = enabled;
        this.useEmulator = useEmulator;
        this.tenant = tenant;
        this.source = source;
        this.topic = topic;
        this.emulatorHostAndPort = emulatorHostAndPort;
        this.credentialsPath = credentialsPath;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean getEnabled() {
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
        final StringBuffer strBuf = new StringBuffer("Zing Config: ");
        strBuf.append(" / enabled = ").append(this.enabled);
        strBuf.append(" / useEmulator = ").append(this.useEmulator);
        strBuf.append(" / emulatorUrl = ").append(this.emulatorHostAndPort);
        strBuf.append(" / tenant = ").append(this.tenant);
        strBuf.append(" / source = ").append(this.source);
        strBuf.append(" / topic = ").append(this.topic);
        strBuf.append(" / credentials path = ").append(this.credentialsPath);
        return strBuf.toString();
    }
}
