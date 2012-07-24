/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.rest;

import org.junit.Test;
import org.zenoss.protobufs.zep.Zep.NumberRange;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import static org.junit.Assert.*;

public class EventsResourceTest {
    @Test
    public void testConvertCount() {
        assertEquals(NumberRange.newBuilder().setFrom(3).build(),
                EventsResource.convertCount(">2"));
        assertEquals(NumberRange.newBuilder().setFrom(5).build(),
                EventsResource.convertCount(">=5"));
        assertEquals(NumberRange.newBuilder().setTo(7).build(),
                EventsResource.convertCount("<=7"));
        assertEquals(NumberRange.newBuilder().setTo(0).build(),
                EventsResource.convertCount("<1"));
        assertEquals(NumberRange.newBuilder().setFrom(17).setTo(17).build(),
                EventsResource.convertCount("17"));
        assertEquals(NumberRange.newBuilder().setFrom(18).setTo(18).build(),
                EventsResource.convertCount("=18"));
        assertEquals(NumberRange.newBuilder().setFrom(5).build(),
                EventsResource.convertCount("5:"));
        assertEquals(NumberRange.newBuilder().setFrom(15).setTo(19).build(),
                EventsResource.convertCount("15:19"));
        assertEquals(NumberRange.newBuilder().setTo(18).build(),
                EventsResource.convertCount(":18"));

        assertNull(EventsResource.convertCount(null));
        assertNull(EventsResource.convertCount(""));

        List<String> failures = Arrays.asList("-17", "=-18", "=>5", "5:4");
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
