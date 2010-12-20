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
package org.zenoss.zep.rest;

import static org.junit.Assert.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.junit.Test;
import org.zenoss.protobufs.zep.Zep.NumberCondition;
import org.zenoss.protobufs.zep.Zep.NumberCondition.Operation;

public class EventsResourceTest {
    @Test
    public void testConvertCount() {
        assertEquals(
                NumberCondition.newBuilder().setOp(Operation.GT).setValue(2)
                        .build(), EventsResource.convertCount(">2"));
        assertEquals(NumberCondition.newBuilder().setOp(Operation.GTEQ)
                .setValue(5).build(), EventsResource.convertCount(">=5"));
        assertEquals(NumberCondition.newBuilder().setOp(Operation.LTEQ)
                .setValue(7).build(), EventsResource.convertCount("<=7"));
        assertEquals(
                NumberCondition.newBuilder().setOp(Operation.LT).setValue(1)
                        .build(), EventsResource.convertCount("<1"));
        assertEquals(
                NumberCondition.newBuilder().setOp(Operation.EQ).setValue(17)
                        .build(), EventsResource.convertCount("17"));
        assertEquals(
                NumberCondition.newBuilder().setOp(Operation.EQ).setValue(18)
                        .build(), EventsResource.convertCount("=18"));

        assertNull(EventsResource.convertCount(null));

        List<String> failures = Arrays.asList("-17", "=-18", "=>5", "");
        for (String failure : failures) {
            try {
                EventsResource.convertCount(failure);
                fail("Expected exception: " + failure);
            } catch (Exception e) {
            }
        }
    }

    @Test
    public void testParseRange() throws ParseException {
        /* Have to remove precision */
        long before = (((System.currentTimeMillis() - 5000) / 1000L) * 1000L);
        long now = ((System.currentTimeMillis() / 1000L) * 1000L);
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        String beforeStr = fmt.format(new Date(before));
        String nowStr = fmt.format(new Date(now));
        assertEquals(before, EventsResource.parseRange(beforeStr)
                .getStartTime());
        assertFalse(EventsResource.parseRange(beforeStr).hasEndTime());
        assertEquals(before, EventsResource
                .parseRange(beforeStr + '/' + nowStr).getStartTime());
        assertEquals(now, EventsResource.parseRange(beforeStr + '/' + nowStr)
                .getEndTime());

        List<String> invalidRanges = Arrays.asList(nowStr + '/' + beforeStr);
        for (String invalidRange : invalidRanges) {
            try {
                EventsResource.parseRange(invalidRange);
                fail("Expected exception on invalid range: " + invalidRange);
            } catch (Exception e) {
                /* Expected */
            }
        }
    }
}
