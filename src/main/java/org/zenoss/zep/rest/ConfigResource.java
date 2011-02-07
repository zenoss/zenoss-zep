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

import org.zenoss.protobufs.ProtobufConstants;
import org.zenoss.protobufs.zep.Zep.EventDetailItem;
import org.zenoss.protobufs.zep.Zep.EventDetailItemSet;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.ConfigDao;
import org.zenoss.zep.dao.EventDetailsConfigDao;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.Map;

@Path("1.0/config")
public class ConfigResource {

    private ConfigDao configDao;
    private EventDetailsConfigDao detailsConfigDao;

    public void setConfigDao(ConfigDao configDao) {
        this.configDao = configDao;
    }

    public void setEventDetailsConfigDao(EventDetailsConfigDao eventDetailsConfigDao) {
        this.detailsConfigDao = eventDetailsConfigDao;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> getConfig() throws ZepException {
        return configDao.getConfig();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public void setConfig(Map<String, String> config) throws ZepException {
        configDao.setConfig(config);
    }

    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    @Path("{name}")
    public Response getConfigValue(@PathParam("name") String name)
            throws ZepException {
        final String value = configDao.getConfigValue(name);
        if (value == null) {
            return Response.status(Status.NOT_FOUND).build();
        }
        return Response.ok(value).build();
    }

    @POST
    @Consumes({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    @Path("{name}")
    public void setConfigValue(@PathParam("name") String name, String value)
            throws ZepException {
        configDao.setConfigValue(name, value);
    }

    @DELETE
    @Path("{name}")
    public Response removeConfigValue(@PathParam("name") String name)
            throws ZepException {
        int result = this.configDao.removeConfigValue(name);
        if (result == 0) {
            return Response.status(Status.NOT_FOUND).build();
        }
        return Response.ok(Status.NO_CONTENT).build();
    }

    @GET
    @Path("index_details")
    @Produces({ MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    public EventDetailItemSet getIndexedDetails() throws ZepException {
        EventDetailItemSet.Builder setBuilder = EventDetailItemSet.newBuilder();
        Map<String, EventDetailItem> indexDetailsMap = this.detailsConfigDao.getEventDetailsIndexConfiguration();
        setBuilder.addAllDetails(indexDetailsMap.values());
        return setBuilder.build();
    }
}
