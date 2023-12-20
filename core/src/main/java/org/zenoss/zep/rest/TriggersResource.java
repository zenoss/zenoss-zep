/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.rest;

import org.jboss.resteasy.annotations.GZIP;
import org.zenoss.protobufs.ProtobufConstants;
import org.zenoss.protobufs.zep.Zep.EventTrigger;
import org.zenoss.protobufs.zep.Zep.EventTriggerSet;
import org.zenoss.protobufs.zep.Zep.EventTriggerSubscriptionSet;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventTriggerDao;
import org.zenoss.zep.dao.EventTriggerSubscriptionDao;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.core.UriInfo;

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
