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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.ProtobufConstants;
import org.zenoss.protobufs.model.Model.Component;
import org.zenoss.protobufs.model.Model.Device;
import org.zenoss.protobufs.model.Model.Service;

@Path("1.0/model")
public class ModelResource {
    private static final Logger logger = LoggerFactory
            .getLogger(ModelResource.class);

    @GET
    @Produces({ MediaType.APPLICATION_JSON,
            ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    @Path("device/{id}")
    public Device getDevice(@PathParam("id") String uuid) {
        logger.info("getDevice: {}", uuid);
        return null;
    }

    @GET
    @Produces({ MediaType.APPLICATION_JSON,
            ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    @Path("component/{id}")
    public Component getComponent(@PathParam("id") String uuid) {
        logger.info("getComponent: {}", uuid);
        return null;
    }

    @GET
    @Produces({ MediaType.APPLICATION_JSON,
            ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    @Path("service/{id}")
    public Service getService(@PathParam("id") String uuid) {
        logger.info("getService: {}", uuid);
        return null;
    }
}
