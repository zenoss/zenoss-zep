package org.zenoss.zep.impl;

import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventTime;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventTimeDao;
import org.zenoss.zep.plugins.EventPostCreateContext;
import org.zenoss.zep.plugins.EventPostCreatePlugin;

public class EventTimePlugin extends EventPostCreatePlugin {

    private EventTimeDao eventTimeDao;

    public void setEventTimeDao(EventTimeDao eventTimeDao) {
        this.eventTimeDao = eventTimeDao;
    }

    @Override
    public void processEvent(Event eventOccurrence, EventSummary event, EventPostCreateContext context) throws ZepException {
        if (event != null) {
            String summaryUuid = event.getUuid();
            long processedTime = event.getUpdateTime();
            long createdTime = eventOccurrence.getCreatedTime();

            EventTime et = EventTime.newBuilder().setProcessedTime(processedTime).setCreatedTime(createdTime).setSummaryUuid(summaryUuid).build();
            this.eventTimeDao.save(et);
        }
    }
}
