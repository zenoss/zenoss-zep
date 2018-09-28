package org.zenoss.zep.zing.impl;

import org.zenoss.protobufs.zep.Zep;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.plugins.EventUpdatePlugin;
import org.zenoss.zep.plugins.EventUpdateContext;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.zep.zing.ZingEventProcessor;

import java.util.List;
import java.util.Arrays;

/*
    Update metadata. All methods receive uuids instead of events, so we need to get
    the full events since Zenoss Cloud does not support partial updates.
*/
public class ZingUpdatePlugin extends EventUpdatePlugin{

    private static final Logger logger = LoggerFactory.getLogger(ZingUpdatePlugin.class);

    private final ZingEventProcessor zingEventProcessor;

    public ZingUpdatePlugin(ZingEventProcessor zingEventProcessor) {
        this.zingEventProcessor = zingEventProcessor;
    }

    public void onStatusUpdate(List<String> uuids, Zep.EventSummaryUpdate update, EventUpdateContext context)
            throws ZepException {
        this.zingEventProcessor.processUpdatedEvents(uuids);
    }

    public void onNoteAdd(String uuid, Zep.EventNote note, EventUpdateContext context) throws ZepException {
        this.zingEventProcessor.processUpdatedEvents(Arrays.asList(uuid));
    }

    public void onEventDetailUpdate(String uuid, Zep.EventDetailSet detSet, EventUpdateContext context) throws ZepException {
        this.zingEventProcessor.processUpdatedEvents(Arrays.asList(uuid));
    }
}
