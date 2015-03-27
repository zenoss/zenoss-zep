package org.zenoss.zep.dao.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.EventSummaryDao;
import org.zenoss.zep.dao.IndexQueueID;
import org.zenoss.zep.dao.impl.EventIndexQueueDaoImpl.PollEvents;
import org.zenoss.zep.index.WorkQueue;
import org.zenoss.zep.index.impl.EventIndexBackendTask;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class SummaryIndexDaoDelegate implements IndexDaoDelegate {
    final private String queueName = EventConstants.TABLE_EVENT_SUMMARY_INDEX;
    final private WorkQueue redisWorkQueue;
    final private EventSummaryDao eventSummaryDao;


    public SummaryIndexDaoDelegate(WorkQueue redisWorkQueue, EventSummaryDao eventSummaryDao) {
        this.redisWorkQueue = redisWorkQueue;
        this.eventSummaryDao = eventSummaryDao;
    }

    @Override
    public PollEvents pollEvents(int limit, long maxUpdateTime) {
        return new RedisPollEvents(limit, maxUpdateTime, eventSummaryDao);
    }

    @Override
    public long getQueueLength() {
        return redisWorkQueue.size();
    }

    @Override
    @TransactionalRollbackAllExceptions
    public void deleteIndexQueueIds(List<IndexQueueID> queueIds) throws ZepException {
        if (!queueIds.isEmpty()) {
            //TODO record metric counter
            //lock on worker
            ArrayList<EventIndexBackendTask> ids = Lists.newArrayListWithCapacity(queueIds.size());
            for (IndexQueueID eid : queueIds) {
                ids.add((EventIndexBackendTask) eid.id);
            }
            redisWorkQueue.completeAll(ids);
        }
    }

    @Override
    public String getQueueName() {
        return queueName;
    }

    private class RedisPollEvents implements PollEvents {
        final private int limit;
        final private long maxUpdateTime;
        final private EventSummaryDao eventSummaryDao;

        private List<EventIndexBackendTask> queueIDs;
        private List<EventSummary> indexed;
        private Set<String> deleted;

        public RedisPollEvents(int limit, long maxUpdateTime, EventSummaryDao eventSummaryDao) {
            this.limit = limit;
            this.maxUpdateTime = maxUpdateTime;
            this.eventSummaryDao = eventSummaryDao;
        }

        @Override
        public List<IndexQueueID> getIndexQueueIds() {
            ArrayList<IndexQueueID> result = Lists.newArrayListWithCapacity(queueIDs.size());
            for (EventIndexBackendTask id : queueIDs) {
                result.add(new IndexQueueID(id));
            }
            return result;
        }

        @Override
        public List<EventSummary> getIndexed() {
            return indexed;
        }

        @Override
        public Set<String> getDeleted() {
            return deleted;
        }

        @Override
        public PollEvents invoke() throws ZepException {
            try {
                queueIDs = redisWorkQueue.poll(limit, 250, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Restore the interrupted status
                Thread.currentThread().interrupt();
                throw new ZepException(e);
            }

            //filter for events smaller than maxUpdateTime
            if (maxUpdateTime > 0) {
                List<EventIndexBackendTask> filteredEventIds = Lists.newArrayListWithCapacity(queueIDs.size());
                for (EventIndexBackendTask et : queueIDs) {
                    if (et.lastSeen > maxUpdateTime) {
                        break;
                    } else {
                        filteredEventIds.add(et);
                    }
                }
                queueIDs = filteredEventIds;
            }

            final Set<String> eventUuids = new HashSet<String>();
            final Set<String> found = new HashSet<String>();

            //clear out dups though there shouldn't be any.
            for (EventIndexBackendTask et : queueIDs) {
                String iqUuid = et.uuid;
                eventUuids.add(iqUuid);
            }

            //read in events and figure out which are deleted
            indexed = this.eventSummaryDao.findByUuids(Lists.newArrayList(eventUuids));
            for (EventSummary es : indexed) {
                found.add(es.getUuid());
            }
            deleted = Sets.difference(eventUuids, found);
            return this;
        }
    }

}
