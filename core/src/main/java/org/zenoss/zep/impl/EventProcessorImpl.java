/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.ZepRawEvent;
import org.zenoss.zep.EventContext;
import org.zenoss.zep.EventPreProcessingPlugin;
import org.zenoss.zep.EventProcessor;
import org.zenoss.zep.PluginService;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventStoreDao;

/**
 * Default implementation of {@link EventProcessor} which uses
 * {@link PluginService} to load the appropriate plug-ins and process events.
 */
public class EventProcessorImpl implements EventProcessor {

    private static final Logger logger = LoggerFactory.getLogger(EventProcessorImpl.class);

    private static final String EVENT_CLASS_UNKNOWN = "/Unknown";

    private PluginService pluginService;

    private EventStoreDao eventStoreDao;

    public void setEventStoreDao(EventStoreDao eventStoreDao) {
        this.eventStoreDao = eventStoreDao;
    }

    /**
     * Sets the plug-in service used to look up configured plug-ins.
     * 
     * @param pluginService
     *            The plug-in service to use to look up configured plug-ins.
     */
    public void setPluginService(PluginService pluginService) {
        this.pluginService = pluginService;
    }

    private static Event eventFromRawEvent(ZepRawEvent zepRawEvent) {
        Event event = zepRawEvent.getEvent();
        // Default to event class unknown.
        if (event.getEventClass().isEmpty()) {
            event = Event.newBuilder(event).setEventClass(EVENT_CLASS_UNKNOWN).build();
        }
        return event;
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void processEvent(ZepRawEvent zepRawEvent) throws ZepException {
        logger.debug("processEvent: event={}", zepRawEvent);

        if (zepRawEvent.getEvent().getStatus() == EventStatus.STATUS_DROPPED) {
            logger.debug("Event dropped: {}", zepRawEvent);
            return;
        } else if (zepRawEvent.getEvent().getUuid().isEmpty()) {
            logger.error("Could not process event, has no uuid: {}",
                    zepRawEvent);
            return;
        } else if (!zepRawEvent.getEvent().hasCreatedTime()) {
            logger.error("Could not process event, has no created_time: {}",
                    zepRawEvent);
            return;
        }

        EventContext ctx = new EventContextImpl(zepRawEvent);

        Event event = eventFromRawEvent(zepRawEvent);

        for (EventPreProcessingPlugin plugin : pluginService.getPreProcessingPlugins()) {
            Event modified = plugin.processEvent(event, ctx);
            if (modified.getStatus() == EventStatus.STATUS_DROPPED) {
                logger.debug("Event dropped by {}", plugin.getName());
                return;
            }

            if (modified != null && !modified.equals(event)) {
                logger.debug("Event modified by {} as {}", plugin.getName(), modified);
                event = modified;
            }
        }

        this.eventStoreDao.create(event, ctx);
    }

}
