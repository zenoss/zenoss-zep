/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2010, Zenoss Inc.
 * 
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 * 
 * For complete information please visit: http://www.zenoss.com/oss/
 */
package org.zenoss.zep.impl;

import org.springframework.transaction.annotation.Transactional;
import org.zenoss.protobufs.modelevents.Modelevents.ModelEvent;
import org.zenoss.protobufs.modelevents.Modelevents.ModelEventList;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventSummaryDao;

public class ModelChangeEventQueueListener extends AbstractEventQueueListener {

    private EventSummaryDao eventSummaryDao;

    @Override
    protected String getQueueIdentifier() {
        return "$ZepModelChange";
    }

    public void setEventSummaryDao(EventSummaryDao eventSummaryDao) {
        this.eventSummaryDao = eventSummaryDao;
    }

    private void processMessage(ModelEventList eventlist) throws ZepException {

        for (ModelEvent event : eventlist.getEventsList()) {

            // only interested in AddedEvent's for Devices
            if (event.getType() == ModelEvent.Type.ADDED && 
	        event.getModelType() == ModelEvent.ModelType.DEVICE) {
                logger.info("Reidentify all events for device: {}", event.getDevice().getId());
                eventSummaryDao.reidentify(event);                
            }
        }

    }

    @Override
    @Transactional
    public void handle(com.google.protobuf.Message message) throws Exception {
        if (!(message instanceof ModelEventList)) {
            logger.warn("Unexpected message type: {}", message);
        } else {
            this.processMessage((ModelEventList)message);
        }

    }

}
