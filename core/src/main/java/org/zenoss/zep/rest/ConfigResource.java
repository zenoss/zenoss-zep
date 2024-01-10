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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.zenoss.protobufs.ProtobufConstants;
import org.zenoss.protobufs.zep.Zep.EventDetailItem;
import org.zenoss.protobufs.zep.Zep.EventDetailItemSet;
import org.zenoss.protobufs.zep.Zep.ZepConfig;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.ConfigDao;
import org.zenoss.zep.dao.EventDetailsConfigDao;
import org.zenoss.zep.events.IndexDetailsUpdatedEvent;
import org.zenoss.zep.events.ZepConfigUpdatedEvent;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.Map;

@Path("1.0/config")
public class ConfigResource implements ApplicationEventPublisherAware {

    //private static final Logger logger = LoggerFactory.getLogger(ConfigResource.class);
    private ConfigDao configDao;
    private EventDetailsConfigDao detailsConfigDao;
    private ApplicationEventPublisher applicationEventPublisher;

    public void setConfigDao(ConfigDao configDao) {
        this.configDao = configDao;
    }

    public void setEventDetailsConfigDao(EventDetailsConfigDao eventDetailsConfigDao) {
        this.detailsConfigDao = eventDetailsConfigDao;
    }

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @GET
    @Produces({MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF})
    @GZIP
    public ZepConfig getConfig() throws ZepException {
        return configDao.getConfig();
    }

    @PUT
    @Consumes({MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF})
    @GZIP
    public void setConfig(ZepConfig config) throws ZepException {
        ZepConfig currentConfig = configDao.getConfig();
        
        configDao.setConfig(config);

        if (!currentConfig.equals(config)) {
            this.applicationEventPublisher.publishEvent(new ZepConfigUpdatedEvent(this, config));
        }
    }

    @PUT
    @Consumes({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN, ProtobufConstants.CONTENT_TYPE_PROTOBUF})
    @Path("{name}")
    @GZIP
    public void setConfigValue(@PathParam("name") String name, ZepConfig value)
            throws ZepException {
        ZepConfig currentConfig = configDao.getConfig();

        configDao.setConfigValue(name, value);

        ZepConfig newConfig = configDao.getConfig();
        if (!currentConfig.equals(newConfig)) {
            this.applicationEventPublisher.publishEvent(new ZepConfigUpdatedEvent(this, newConfig));
        }
    }

    @DELETE
    @Path("{name}")
    @GZIP
    public Response removeConfigValue(@PathParam("name") String name)
            throws ZepException {
        ZepConfig currentConfig = configDao.getConfig();

        int result = this.configDao.removeConfigValue(name);
        if (result == 0) {
            return Response.status(Status.NOT_FOUND).build();
        }

        ZepConfig newConfig = configDao.getConfig();
        if (!currentConfig.equals(newConfig)) {
            this.applicationEventPublisher.publishEvent(new ZepConfigUpdatedEvent(this, newConfig));
        }
        
        return Response.status(Status.NO_CONTENT).build();
    }

    @GET
    @Path("index_details")
    @Produces({MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF})
    @GZIP
    public EventDetailItemSet getIndexedDetails() throws ZepException {
        EventDetailItemSet.Builder setBuilder = EventDetailItemSet.newBuilder();
        Map<String, EventDetailItem> indexDetailsMap = this.detailsConfigDao.getEventDetailItemsByName();
        setBuilder.addAllDetails(indexDetailsMap.values());
        return setBuilder.build();
    }

    @GET
    @Path("index_details/{detail_name}")
    @Produces({MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF})
    @GZIP
    public Response getIndexedDetailByName(@PathParam("detail_name") String detailName) throws ZepException {
        EventDetailItem item = this.detailsConfigDao.findByName(detailName);
        final Response response;
        if (item == null) {
            response = Response.status(Status.NOT_FOUND).build();
        } else {
            response = Response.ok(item).build();
        }
        return response;
    }

    @PUT
    @Path("index_details/{detail_name}")
    @Consumes({MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF})
    @GZIP
    public Response createIndexedDetail(@PathParam("detail_name") String detailName, EventDetailItem item)
            throws ZepException {
        if (!detailName.equals(item.getKey())) {
            throw new ZepException(String.format("Detail name doesn't match URI: %s != %s", detailName, item.getKey()));
        }

        EventDetailItemSet set = EventDetailItemSet.newBuilder().addDetails(item).build();
        return createIndexedDetails(set);
    }

    @POST
    @Path("index_details")
    @Consumes({MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF})
    @GZIP
    public Response createIndexedDetails(EventDetailItemSet items) throws ZepException {
        if (items.getDetailsCount() == 0) {
            return Response.status(Status.BAD_REQUEST).build();
        }

        Map<String,EventDetailItem> currentItems = this.detailsConfigDao.getEventDetailItemsByName();
        
        for (EventDetailItem item : items.getDetailsList()) {
            this.detailsConfigDao.create(item);
        }

        Map<String,EventDetailItem> newItems = this.detailsConfigDao.getEventDetailItemsByName();
        if (!currentItems.equals(newItems)) {
            this.applicationEventPublisher.publishEvent(new IndexDetailsUpdatedEvent(this, newItems));
        }
        
        return Response.status(Status.ACCEPTED).build();
    }

    @DELETE
    @Path("index_details/{detail_name}")
    @GZIP
    public Response deleteIndexedDetail(@PathParam("detail_name") String detailName) throws ZepException {
        final int numRows = this.detailsConfigDao.delete(detailName);
        final Response response;
        if (numRows == 0) {
            response = Response.status(Status.NOT_FOUND).build();
        } else {
            response = Response.noContent().build();
            
            Map<String,EventDetailItem> newItems = this.detailsConfigDao.getEventDetailItemsByName();
            this.applicationEventPublisher.publishEvent(new IndexDetailsUpdatedEvent(this, newItems));
        }
        return response;
    }
}
