/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventTime;
import org.zenoss.zep.UUIDGenerator;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventSummaryDao;
import org.zenoss.zep.dao.EventTimeDao;
import org.zenoss.zep.impl.EventPreCreateContextImpl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


@ContextConfiguration({ "classpath:zep-config.xml" })
public class EventTimeDaoIT extends
        AbstractTransactionalJUnit4SpringContextTests {

    @Autowired
    public EventTimeDao dao;

    @Autowired
    public EventSummaryDao eventSummaryDao;


    @Autowired
    public UUIDGenerator uuidGenerator;

    private EventTime createEventTime(){
        long time = System.currentTimeMillis();
        return createEventTime(time);
    }

    private EventTime createEventTime(long processedTime){
        String uuid = uuidGenerator.generate().toString();

        return EventTime.newBuilder().setCreatedTime(processedTime).setProcessedTime(processedTime).setFirstSeenTime(processedTime).setSummaryUuid(uuid).build();

    }


    @Test
    public void testInsert() throws ZepException {

        EventTime evTime = createEventTime();
        long time = evTime.getProcessedTime();
        dao.save(evTime);

        List<EventTime> result = dao.findProcessedSince(new Date(time), 100);
        assertNotNull(result);
        assertEquals(1, result.size());
        EventTime timeResult = result.iterator().next();
        assertEquals(evTime, timeResult);

    }


    @Test
    public void testFindProcessedSinceDateGreater() throws Exception {
        int limit = 10;
        List<EventTime> et = new ArrayList<EventTime>(limit);
        for(int i = 0; i< limit; i++){
            EventTime evTime = createEventTime();
            et.add(evTime);
            dao.save(evTime);
        }

        //test using a time after all the events are processed
        long time = System.currentTimeMillis() + 1000;
        List<EventTime> result = dao.findProcessedSince(new Date(time), 100);
        assertNotNull(result);
        assertEquals(0, result.size());

    }


    @Test
    public void testFindProcessedSince() throws Exception {
        int limit = 10;
        List<EventTime> et = new ArrayList<EventTime>(limit);
        long time = System.currentTimeMillis();
        for(int i = 0; i< limit; i++){
            //Make sure each event has a unique time so the sort properly when the query returns them
            EventTime evTime = createEventTime(time + i);
            et.add(evTime);
            dao.save(evTime);
        }

        //test using the time from first event time
        time = et.get(0).getProcessedTime();
        List<EventTime> result = dao.findProcessedSince(new Date(time), 100);
        assertNotNull(result);
        assertEquals(10, result.size());
        assertEquals(et, result);
    }

    @Test
    public void testFindProcessedSinceLimit() throws Exception {
        int limit = 10;
        List<EventTime> et = new ArrayList<EventTime>(limit);
        long processedTime = System.currentTimeMillis();
        for(int i = 0; i< limit; i++){
            //Make sure each event has a unique time so they sort properly  when the query returns them
            EventTime evTime = createEventTime(processedTime + i);
            et.add(evTime);
            dao.save(evTime);
        }

        //test using time from the first event time
        long time = et.get(0).getProcessedTime();
        List<EventTime> result = dao.findProcessedSince(new Date(time), 2);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals(et.subList(0,2), result);

        //test using time from the 5th event time
        time = et.get(4).getProcessedTime();
        result = dao.findProcessedSince(new Date(time), 2);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals(et.subList(4,6), result);


    }


}
