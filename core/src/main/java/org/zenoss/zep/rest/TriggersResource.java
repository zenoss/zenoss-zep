/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.rest;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.jboss.resteasy.annotations.GZIP;
import org.zenoss.protobufs.ProtobufConstants;
import org.zenoss.protobufs.zep.Zep.EventTrigger;
import org.zenoss.protobufs.zep.Zep.EventTriggerSet;
import org.zenoss.protobufs.zep.Zep.EventTriggerSubscriptionSet;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventTriggerDao;
import org.zenoss.zep.dao.EventTriggerSubscriptionDao;

/**
 * REST API used to manage event triggers.
 */
@Path("1.0/triggers")
public class TriggersResource {
    private EventTriggerDao eventTriggerDao;
    private EventTriggerSubscriptionDao eventTriggerSubscriptionDao;

    public void setEventTriggerDao(EventTriggerDao eventTriggerDao) {
        this.eventTriggerDao = eventTriggerDao;
    }

    public void setEventTriggerSubscriptionDao(
            EventTriggerSubscriptionDao eventTriggerSubscriptionDao) {
        this.eventTriggerSubscriptionDao = eventTriggerSubscriptionDao;
    }

    @DELETE
    @Path("{triggerUuid}")
    @GZIP
    public Response deleteTrigger(@PathParam("triggerUuid") String triggerUuid)
            throws ZepException {
        int numRows = this.eventTriggerDao.delete(triggerUuid);
        if (numRows == 0) {
            return Response.status(Status.NOT_FOUND).build();
        }
        return Response.noContent().build();
    }

    @GET
    @Path("{triggerUuid}")
    @Produces({ MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    @GZIP
    public Response findTriggerByUuid(
            @PathParam("triggerUuid") String triggerUuid) throws ZepException {
        EventTrigger trigger = this.eventTriggerDao.findByUuid(triggerUuid);
        if (trigger == null) {
            return Response.status(Status.NOT_FOUND).build();
        }
        return Response.ok(trigger).build();
    }

    @GET
    @Path("/")
    @Produces({ MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    @GZIP
    public EventTriggerSet findTriggers() throws ZepException {
        return EventTriggerSet.newBuilder()
                .addAllTriggers(this.eventTriggerDao.findAll()).build();
    }

    @PUT
    @Path("{triggerUuid}")
    @Consumes({ MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    @GZIP
    public Response modifyTrigger(@PathParam("triggerUuid") String triggerUuid,
            EventTrigger trigger, @Context UriInfo info) throws ZepException {
        EventTrigger existing = this.eventTriggerDao.findByUuid(triggerUuid);
        if (existing == null) {
            this.eventTriggerDao.create(trigger);
            return Response.created(info.getAbsolutePath()).build();
        }
        this.eventTriggerDao.modify(trigger);
        return Response.noContent().build();
    }

    @GET
    @Path("subscriptions")
    @Produces({ MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    @GZIP
    public EventTriggerSubscriptionSet findAllSubscriptions()
            throws ZepException {
        return EventTriggerSubscriptionSet
                .newBuilder()
                .addAllSubscriptions(this.eventTriggerSubscriptionDao.findAll())
                .build();
    }

    @GET
    @Path("subscriptions/{subscriber_uuid}")
    @Produces({ MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    @GZIP
    public EventTriggerSubscriptionSet findSubscriptions(
            @PathParam("subscriber_uuid") String subscriberUuid)
            throws ZepException {
        return EventTriggerSubscriptionSet
                .newBuilder()
                .addAllSubscriptions(
                        this.eventTriggerSubscriptionDao
                                .findBySubscriberUuid(subscriberUuid)).build();
    }

    @PUT
    @Path("subscriptions/{subscriber_uuid}")
    @Consumes({ MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    @GZIP
    public Response updateSubscriptions(
            @PathParam("subscriber_uuid") String subscriberUuid,
            EventTriggerSubscriptionSet subscriptions) throws ZepException {
        this.eventTriggerSubscriptionDao.updateSubscriptions(subscriberUuid,
                subscriptions.getSubscriptionsList());
        return Response.noContent().build();
    }
}
