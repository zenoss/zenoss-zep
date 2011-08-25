/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao;

import org.zenoss.protobufs.zep.Zep;

import java.util.Date;
import java.util.List;

/**
 * DAO which provides an interface to the event times table.
 */
public interface EventTimeDao extends Partitionable, Purgable {

    /**
     * Find event time occurrences processed since a given time.
     *
     * @param startDate starting date to fetch events, inclusive
     * @param limit  limit the amount of results
     * @return List of EventTime ordered by processed time ascending
     */
    public List<Zep.EventTime> findProcessedSince(Date startDate, int limit);

    /**
     * save an event time instances
     *
     * @param eventTime
     */
    public void save(Zep.EventTime eventTime);

}
