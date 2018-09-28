package org.zenoss.zep.zing.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.plugins.EventPostCreatePlugin;
import org.zenoss.zep.plugins.EventPostCreateContext;

import org.zenoss.zep.zing.ZingEventProcessor;


public class ZingPostCreatePlugin extends EventPostCreatePlugin {

    private static final Logger logger = LoggerFactory.getLogger(ZingPostCreatePlugin.class);

    private final ZingEventProcessor zingEventProcessor;

    public ZingPostCreatePlugin(ZingEventProcessor zingEventProcessor) {
        this.zingEventProcessor = zingEventProcessor;
    }

    @Override
    public void processEvent(Event eventOccurrence, EventSummary event, EventPostCreateContext context) throws ZepException {
        // if eventOccurrence is a clear event, the EventSummary is null
        try {
            this.zingEventProcessor.processEvent(eventOccurrence, event);
        } catch (Exception e) {
            logger.info("PACOO ESTO PETAAA 2 {}", e);
            throw new ZepException(e);
        }
    }
}
