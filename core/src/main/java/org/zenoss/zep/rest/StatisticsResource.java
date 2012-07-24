/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2012, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.rest;

import org.jboss.resteasy.annotations.GZIP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.ProtobufConstants;
import org.zenoss.protobufs.zep.Zep.ZepStatistic;
import org.zenoss.protobufs.zep.Zep.ZepStatistics;
import org.zenoss.zep.StatisticsService;

import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

@Path("1.0/stats")
public class StatisticsResource {

    private static final Logger logger = LoggerFactory.getLogger(StatisticsResource.class);

    private StatisticsService statisticsService;

    public void setStatisticsService(final StatisticsService service) {
        this.statisticsService = service;
    }

    @GET
    @Path("/")
    @Produces({ MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    @GZIP
    public ZepStatistics get() {
        ZepStatistics.Builder container = ZepStatistics.newBuilder();
        MBeanInfo mBeanInfo = statisticsService.getMBeanInfo();
        for (MBeanAttributeInfo info : mBeanInfo.getAttributes()) {
            String name = info.getName();
            try {
                container.addStats(getZepStat(name));
            } catch (JMException e) {
                logger.warn(String.format("Unable to get stat %s", name));
                continue;
            }
        }
        return container.build();
    }

    @GET
    @Path("{name}")
    @Produces({MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF})
    @GZIP
    public Response getStat(@PathParam("name") String name) {
        try {
            ZepStatistic stat = getZepStat(name);
            return Response.ok(stat).build();
        } catch (JMException e) {
            logger.warn(String.format("Unable to get stat %s", name));
            return Response.status(Status.NOT_FOUND).build();
        }
    }

    private ZepStatistic getZepStat(String name) throws JMException {
        long value = (Long) statisticsService.getAttribute(name);
        ZepStatistic.Builder stat = ZepStatistic.newBuilder();
        stat.setName(name);
        stat.setDescription(statisticsService.getAttributeDescription(name));
        stat.setValue(value);
        return stat.build();
    }

}
