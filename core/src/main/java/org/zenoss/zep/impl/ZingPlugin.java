package org.zenoss.zep.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.zep.Zep;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.plugins.EventPostIndexContext;
import org.zenoss.zep.plugins.EventPostIndexPlugin;
import org.zenoss.zep.zing.ZingEventProcessor;


public class ZingPlugin extends EventPostIndexPlugin {

    private static final Logger logger = LoggerFactory.getLogger(ZingPlugin.class);

    private final ZingEventProcessor zingEventProcessor;

    public ZingPlugin(ZingEventProcessor zingEventProcessor) {
        this.zingEventProcessor = zingEventProcessor;
    }

    public void processEvent(Zep.EventSummary eventSummary, EventPostIndexContext context) throws ZepException {
        zingEventProcessor.processEvent(eventSummary);
    }
}
