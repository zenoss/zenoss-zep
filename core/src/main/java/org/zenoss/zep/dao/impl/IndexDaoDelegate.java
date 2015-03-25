package org.zenoss.zep.dao.impl;

import org.zenoss.zep.ZepException;
import org.zenoss.zep.annotations.TransactionalRollbackAllExceptions;
import org.zenoss.zep.dao.IndexQueueID;
import org.zenoss.zep.dao.impl.EventIndexQueueDaoImpl.PollEvents;

import java.util.List;

/**
 * Created by jplouis on 3/25/15.
 */
public interface IndexDaoDelegate {
    PollEvents pollEvents(int limit, long maxUpdateTime);

    void queueEvents(List<String> uuids, long timestamp);

    long getQueueLength();

    void deleteIndexQueueIds(List<IndexQueueID> queueIds) throws ZepException;

    String getQueueName();
}
