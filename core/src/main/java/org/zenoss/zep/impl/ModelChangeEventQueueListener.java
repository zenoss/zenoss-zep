/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.model.Model.ModelElementType;
import org.zenoss.protobufs.modelevents.Modelevents.ModelEvent;
import org.zenoss.protobufs.modelevents.Modelevents.ModelEventList;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventSummaryDao;

public class ModelChangeEventQueueListener extends AbstractEventQueueListener {

    private static final Logger logger = LoggerFactory.getLogger(ModelChangeEventQueueListener.class);

    private EventSummaryDao eventSummaryDao;

    @Override
    protected String getQueueIdentifier() {
        return "$ZepModelChange";
    }

    public void setEventSummaryDao(EventSummaryDao eventSummaryDao) {
        this.eventSummaryDao = eventSummaryDao;
    }

    private void processModelAdded(ModelEvent event) throws ZepException {
        final ModelElementType type = event.getModelType();
        String id = null, uuid = null, parentId = null, parentUuid = null;
        switch (event.getModelType()) {
            case COMPONENT:
                id = event.getComponent().getId();
                uuid = event.getComponent().getUuid();
                parentId = event.getComponent().getDevice().getId();
                parentUuid = event.getComponent().getDevice().getUuid();
                break;
            case DEVICE:
                id = event.getDevice().getId();
                uuid = event.getDevice().getUuid();
                break;
        }
        if (id != null && uuid != null) {
            if (parentId != null) {
                logger.info("Re-identifying events for {}, {}", parentId, id);
            }
            else {
                logger.info("Re-identifying events for {}", id);
            }
            this.eventSummaryDao.reidentify(type, id, uuid, parentUuid);
        }
    }

    private void processModelRemoved(ModelEvent event) throws ZepException {
        String uuid = null;
        switch (event.getModelType()) {
            case COMPONENT:
                uuid = event.getComponent().getUuid();
                break;
            case DEVICE:
                uuid = event.getDevice().getUuid();
                break;
            case SERVICE:
                uuid = event.getService().getUuid();
                break;
        }
        if (uuid != null) {
            logger.info("De-identifying events for {}", uuid);
            eventSummaryDao.deidentify(uuid);
        }
    }

    private void processMessage(ModelEventList eventlist) throws ZepException {
        for (ModelEvent event : eventlist.getEventsList()) {
            switch (event.getType()) {
                case ADDED:
                    processModelAdded(event);
                    break;
                case REMOVED:
                    processModelRemoved(event);
                    break;
            }
        }
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void handle(com.google.protobuf.Message message) throws Exception {
        if (!(message instanceof ModelEventList)) {
            logger.warn("Unexpected message type: {}", message);
        } else {
            this.processMessage((ModelEventList)message);
        }
    }

}
