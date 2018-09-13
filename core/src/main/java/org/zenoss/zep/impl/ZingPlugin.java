package org.zenoss.zep.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.zep.Zep;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.plugins.EventPostIndexContext;
import org.zenoss.zep.plugins.EventPostIndexPlugin;

public class ZingPlugin extends EventPostIndexPlugin {

    private static final Logger logger = LoggerFactory.getLogger(ZingPlugin.class);

    private boolean enabled;

    public ZingPlugin(boolean enabled) {
        this.enabled = enabled;
    }

    public void processEvent(Zep.EventSummary eventSummary, EventPostIndexContext context) throws ZepException {

        String action = "FORWARD";
        if (!this.enabled) {
            action = "DROP";
        }
        logger.info(" We should {} event {} to ZingConnector", action, eventSummary.getUuid());
    }
}
