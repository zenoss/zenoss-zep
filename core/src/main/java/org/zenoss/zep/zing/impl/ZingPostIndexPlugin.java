package org.zenoss.zep.zing.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.plugins.EventPostIndexPlugin;
import org.zenoss.zep.plugins.EventPostIndexContext;

import org.zenoss.zep.zing.ZingEventProcessor;

import java.util.ArrayList;
import java.util.List;


public class ZingPostIndexPlugin extends EventPostIndexPlugin {

    private static final Logger logger = LoggerFactory.getLogger(ZingPostIndexPlugin.class);

    private final ZingEventProcessor zingEventProcessor;

    public ZingPostIndexPlugin(ZingEventProcessor zingEventProcessor) {
        this.zingEventProcessor = zingEventProcessor;
    }

    private boolean forwardEvents(EventPostIndexContext context) {
        return this.zingEventProcessor.enabled() && !context.isArchive();
    }

    @Override
    public void startBatch(EventPostIndexContext context) throws Exception {
        if (this.forwardEvents(context)) {
            context.setPluginState(this, new ArrayList<EventSummary>(context.getIndexLimit()));
        }
    }

    @Override
    public void processEvent(EventSummary eventSummary, EventPostIndexContext context) throws ZepException {
        if (this.forwardEvents(context)) {
            List<EventSummary> events = (List<EventSummary>) context.getPluginState(this);
            events.add(eventSummary);
        }
    }

    @Override
    public void endBatch(EventPostIndexContext context) throws Exception {
        if (this.forwardEvents(context)) {
            List<EventSummary> events = (List<EventSummary>) context.getPluginState(this);
            for (EventSummary eventSummary : events) {
                try {
                    this.zingEventProcessor.processEvent(eventSummary);
                } catch (Exception e) {
                    // FIXME improve error handling
                    logger.error("Exception sending events to Zenoss Cloud. {}", e);
                }
            }
            context.setPluginState(this, null);
        }
    }
}
