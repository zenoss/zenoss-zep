/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.rest;

import org.zenoss.protobufs.ProtobufConstants;
import org.zenoss.protobufs.zep.Zep.DaemonHeartbeatSet;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.HeartbeatDao;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * REST API for heartbeat information.
 */
@Path("1.0/heartbeats")
public class HeartbeatsResource {

    private HeartbeatDao heartbeatDao;

    public void setHeartbeatDao(HeartbeatDao heartbeatDao) {
        this.heartbeatDao = heartbeatDao;
    }

    @GET
    @Path("/")
    @Produces({ MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    public DaemonHeartbeatSet getAllHeartbeats() throws ZepException {
        DaemonHeartbeatSet.Builder dhb = DaemonHeartbeatSet.newBuilder();
        dhb.addAllHeartbeats(heartbeatDao.findAll());
        return dhb.build();
    }

    @GET
    @Path("/{monitor}")
    @Produces({ MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    public DaemonHeartbeatSet getHeartbeatsForMonitor(@PathParam("monitor") String monitor) throws ZepException {
        DaemonHeartbeatSet.Builder dhb = DaemonHeartbeatSet.newBuilder();
        dhb.addAllHeartbeats(heartbeatDao.findByMonitor(monitor));
        return dhb.build();
    }

    @DELETE
    @Path("/")
    public Response deleteAllHeartbeats() throws ZepException {
        heartbeatDao.deleteAll();
        return Response.noContent().build();
    }

    @DELETE
    @Path("/{monitor}")
    public Response deleteByMonitor(@PathParam("monitor") String monitor) throws ZepException {
        heartbeatDao.deleteByMonitor(monitor);
        return Response.noContent().build();
    }
}
