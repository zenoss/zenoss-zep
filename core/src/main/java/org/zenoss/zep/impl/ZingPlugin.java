package org.zenoss.zep.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.plugins.EventPostCreatePlugin;
import org.zenoss.zep.plugins.EventPostCreateContext;

import org.zenoss.zep.zing.ZingEventProcessor;


public class ZingPlugin extends EventPostCreatePlugin {

    private static final Logger logger = LoggerFactory.getLogger(ZingPlugin.class);

    private final ZingEventProcessor zingEventProcessor;

    public ZingPlugin(ZingEventProcessor zingEventProcessor) {
        this.zingEventProcessor = zingEventProcessor;
    }

    @Override
    public void processEvent(Event eventOccurrence, EventSummary event, EventPostCreateContext context) throws ZepException {
        this.zingEventProcessor.processEvent(eventOccurrence, event);
    }
}
