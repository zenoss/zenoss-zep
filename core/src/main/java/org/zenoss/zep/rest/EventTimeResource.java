/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */

package org.zenoss.zep.rest;

import org.jboss.resteasy.annotations.GZIP;
import org.zenoss.protobufs.ProtobufConstants;
import org.zenoss.protobufs.zep.Zep.EventTime;
import org.zenoss.protobufs.zep.Zep.EventTimeSet;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventTimeDao;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.Date;
import java.util.List;

/**
 * REST API for heartbeat information.
 */
@Path("1.0/eventtime")
public class EventTimeResource {

    private EventTimeDao eventTimeDao;

    public void setEventTimeDao(EventTimeDao eventTimeDao) {
        this.eventTimeDao = eventTimeDao;
    }

    @GET
    @Path("/since")
    @Produces({MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF})
    @GZIP
    public EventTimeSet getEventTimesSince(@QueryParam("time") Long time, @QueryParam("limit") Integer limit) throws ZepException {
        Date date = new Date(time);
        if (limit == null) {
            limit = 1000;
        }
        List<EventTime> results = this.eventTimeDao.findProcessedSince(date, limit);
        EventTimeSet ets = EventTimeSet.newBuilder().addAllEventTimes(results).build();
        return ets;
    }

}
