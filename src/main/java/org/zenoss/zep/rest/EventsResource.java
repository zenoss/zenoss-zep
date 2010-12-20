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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.ProtobufConstants;
import org.zenoss.protobufs.util.Util.TimestampRange;
import org.zenoss.protobufs.zep.Zep.EventFilter;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventSort;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventSummaryFilter;
import org.zenoss.protobufs.zep.Zep.EventSummaryRequest;
import org.zenoss.protobufs.zep.Zep.EventSummaryResult;
import org.zenoss.protobufs.zep.Zep.EventSummaryUpdate;
import org.zenoss.protobufs.zep.Zep.EventSummaryUpdateRequest;
import org.zenoss.protobufs.zep.Zep.EventSummaryUpdateResponse;
import org.zenoss.protobufs.zep.Zep.EventTagSeverities;
import org.zenoss.protobufs.zep.Zep.EventTagSeveritiesSet;
import org.zenoss.protobufs.zep.Zep.EventTagSeverity;
import org.zenoss.protobufs.zep.Zep.FilterOperator;
import org.zenoss.protobufs.zep.Zep.NumberCondition;
import org.zenoss.protobufs.zep.Zep.NumberCondition.Operation;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.EventStoreDao;
import org.zenoss.zep.index.EventIndexer;

@Path("1.0/events")
public class EventsResource {
    private static final Logger logger = LoggerFactory
            .getLogger(EventsResource.class);

    private Integer defaultQueryLimit = Integer.valueOf(100);
    private EventIndexer eventIndexer;

    public void setDefaultQueryLimit(Integer limit) {
        this.defaultQueryLimit = limit;
    }

    private EventStoreDao eventStoreDao;

    public void setEventStoreDao(EventStoreDao eventStoreDao) {
        this.eventStoreDao = eventStoreDao;
    }

    public void setEventIndexer(EventIndexer eventIndexer) {
        this.eventIndexer = eventIndexer;
    }

    private static Set<String> getQuerySet(
            MultivaluedMap<String, String> params, String name) {
        final Set<String> set;
        final List<String> l = params.get(name);
        if (l == null) {
            set = Collections.emptySet();
        } else {
            set = new HashSet<String>(l.size());
            set.addAll(l);
        }
        return set;
    }

    private static int getQueryInteger(MultivaluedMap<String, String> params,
            String name, int defaultValue) {
        final String strVal = params.getFirst(name);
        return (strVal != null) ? Integer.valueOf(strVal) : defaultValue;
    }

    static <T extends Enum<T>> EnumSet<T> getQueryEnumSet(
            MultivaluedMap<String, String> params, String name,
            Class<T> enumClass, String prefix) {
        final EnumSet<T> set = EnumSet.noneOf(enumClass);
        final List<String> values = params.get(name);
        if (values != null) {
            for (String value : values) {
                value = value.toUpperCase();
                if (!value.startsWith(prefix)) {
                    value = prefix + value;
                }

                set.add(Enum.valueOf(enumClass, value));
            }
        }

        return set;
    }

    @GET
    @Path("{eventUuid}")
    @Produces({ MediaType.APPLICATION_JSON,
            ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    public Response getEventSummaryByUuid(
            @PathParam("eventUuid") String eventUuid) throws ZepException {
        EventSummary summary = eventStoreDao.findByUuid(eventUuid);
        if (summary == null) {
            return Response.status(Status.NOT_FOUND).build();
        }

        return Response.ok(summary).build();
    }

    @PUT
    @Path("")
    @Consumes({ MediaType.APPLICATION_JSON,
            ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    @Produces({ MediaType.APPLICATION_JSON,
            ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    public EventSummaryUpdateResponse updateEventSummary(
            EventSummaryUpdateRequest request) throws ZepException {
        final long updateTime;
        if (!request.hasUpdateTime()) {
            updateTime = System.currentTimeMillis();
            // Force an index to ensure we have a consistent view of data
            this.eventIndexer.index(true);
        } else {
            updateTime = request.getUpdateTime();
        }

        EventSummaryRequest.Builder reqBuilder = EventSummaryRequest
                .newBuilder();

        // We only want UUIDs returned
        // TODO: Constant / Enum
        reqBuilder.addKeys("uuid");

        // Add additional constraint on user's filter that we only want to
        // include events with update_time < the current time. This prevents
        // infinite updates and updating events that are modified after the
        // request comes in.
        EventFilter.Builder filterBuilder = EventFilter.newBuilder(request
                .getEventFilter());
        filterBuilder.addUpdateTime(TimestampRange.newBuilder().setEndTime(
                updateTime));
        EventFilter filter = filterBuilder.build();
        reqBuilder.setEventFilter(filter);

        // Maximum number of events we will update in one batch
        reqBuilder.setLimit(Math.min(request.getLimit(), defaultQueryLimit));

        EventSummaryRequest req = reqBuilder.build();

        EventSummaryResult result = this.eventStoreDao.list(req);
        List<String> uuids = new ArrayList<String>(result.getEventsCount());
        for (EventSummary summary : result.getEventsList()) {
            uuids.add(summary.getUuid());
        }
        eventStoreDao.update(uuids, request.getUpdateFields());

        // Force the index to update - this will prevent updating these events again
        eventIndexer.index(true);

        EventSummaryUpdateResponse.Builder response = EventSummaryUpdateResponse
                .newBuilder();
        response.setRemaining(result.getTotal() - result.getEventsCount());
        response.setUpdated(result.getEventsCount());
        response.setRequest(EventSummaryUpdateRequest.newBuilder(request)
                .setUpdateTime(updateTime).build());
        return response.build();
    }

    @PUT
    @Path("{eventUuid}")
    @Consumes({ MediaType.APPLICATION_JSON,
            ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    public Response updateEventSummaryByUuid(
            @PathParam("eventUuid") String uuid, EventSummaryUpdate update)
            throws ZepException {

        EventSummary summary = eventStoreDao.findByUuid(uuid);
        if (summary == null) {
            return Response.status(Status.NOT_FOUND).build();
        }

        int numRows = eventStoreDao.update(summary.getUuid(), update);
        if (numRows > 0) {
            this.eventIndexer.index();
        }

        return Response.noContent().build();
    }

    @POST
    @Path("{eventUuid}/notes")
    @Consumes({ MediaType.APPLICATION_JSON,
            ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    public Response addNote(@PathParam("eventUuid") String eventUuid,
            EventNote note) throws ZepException {

        EventSummary summary = eventStoreDao.findByUuid(eventUuid);
        if (summary == null) {
            return Response.status(Status.NOT_FOUND).build();
        }

        int numRows = eventStoreDao.addNote(summary.getUuid(), note);
        if (numRows == 0) {
            return Response.status(Status.INTERNAL_SERVER_ERROR).build();
        }

        return Response.noContent().build();
    }

    @GET
    @Produces({ MediaType.APPLICATION_JSON,
            ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    public EventSummaryResult listEventIndex(@Context UriInfo ui)
            throws ParseException, JsonGenerationException,
            JsonMappingException, IOException, ZepException {
        return eventStoreDao.list(eventSummaryRequestFromUriInfo(ui));
    }

    @GET
    @Path("archive")
    @Produces({ MediaType.APPLICATION_JSON,
            ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    public EventSummaryResult listEventIndexArchive(@Context UriInfo ui)
            throws ParseException, JsonGenerationException,
            JsonMappingException, IOException, ZepException {
        return eventStoreDao.listArchive(eventSummaryRequestFromUriInfo(ui));
    }

    @GET
    @Path("severities")
    @Produces({ MediaType.APPLICATION_JSON,
            ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    public EventTagSeveritiesSet listEventSeverities(@Context UriInfo ui)
            throws ZepException {
        final MultivaluedMap<String, String> queryParams = ui
                .getQueryParameters();
        Set<String> uuids = getQuerySet(queryParams, "tag");
        EventTagSeveritiesSet.Builder setBuilder = EventTagSeveritiesSet
                .newBuilder();
        if (!uuids.isEmpty()) {
            Map<String, Map<EventSeverity, Integer>> counts = eventStoreDao
                    .countSeverities(uuids);
            for (Map.Entry<String, Map<EventSeverity, Integer>> entry : counts
                    .entrySet()) {
                EventTagSeverities.Builder sevsBuilder = EventTagSeverities
                        .newBuilder().setTagUuid(entry.getKey());
                for (Map.Entry<EventSeverity, Integer> severityEntry : entry
                        .getValue().entrySet()) {
                    sevsBuilder.addSeverities(EventTagSeverity.newBuilder()
                            .setSeverity(severityEntry.getKey())
                            .setCount(severityEntry.getValue()).build());
                }
                setBuilder.addSeverities(sevsBuilder.build());
            }
        }
        return setBuilder.build();
    }

    @GET
    @Path("worst_severity")
    @Produces({ MediaType.APPLICATION_JSON,
            ProtobufConstants.CONTENT_TYPE_PROTOBUF })
    public EventTagSeveritiesSet listWorstEventSeverity(@Context UriInfo ui)
            throws ZepException {
        final MultivaluedMap<String, String> queryParams = ui
                .getQueryParameters();
        Set<String> uuids = getQuerySet(queryParams, "tag");
        EventTagSeveritiesSet.Builder setBuilder = EventTagSeveritiesSet
                .newBuilder();
        if (!uuids.isEmpty()) {
            Map<String, EventSeverity> worstSeverities = eventStoreDao
                    .findWorstSeverity(uuids);
            for (Map.Entry<String, EventSeverity> entry : worstSeverities
                    .entrySet()) {
                EventTagSeverities.Builder sevsBuilder = EventTagSeverities
                        .newBuilder().setTagUuid(entry.getKey());
                sevsBuilder.addSeverities(EventTagSeverity.newBuilder()
                        .setSeverity(entry.getValue()).build());
                setBuilder.addSeverities(sevsBuilder.build());
            }
        }
        return setBuilder.build();
    }

    private EventSummaryRequest eventSummaryRequestFromUriInfo(UriInfo info)
            throws ParseException {
        /* Read all params from query */
        MultivaluedMap<String, String> queryParams = info.getQueryParameters();
        logger.debug("Query Parameters: {}", queryParams);
        final Set<String> keys = getQuerySet(queryParams, "keys");
        final int limit = getQueryInteger(queryParams, "limit",
                defaultQueryLimit);
        final int offset = getQueryInteger(queryParams, "offset", 0);
        final EnumSet<EventSeverity> severities = getQueryEnumSet(queryParams,
                "severity", EventSeverity.class, "SEVERITY_");
        final EnumSet<EventStatus> status = getQueryEnumSet(queryParams,
                "status", EventStatus.class, "STATUS_");
        final String eventClass = queryParams.getFirst("event_class");
        final TimestampRange firstSeen = parseRange(queryParams
                .getFirst("first_seen"));
        final TimestampRange lastSeen = parseRange(queryParams
                .getFirst("last_seen"));
        final TimestampRange statusChange = parseRange(queryParams
                .getFirst("status_change"));
        final NumberCondition count = convertCount(queryParams
                .getFirst("count"));
        final Set<String> tagUuids = getQuerySet(queryParams, "tag_uuids");
        final String summary = queryParams.getFirst("event_summary");
        final String element_identifier = queryParams
                .getFirst("element_identifier");
        final String element_sub_identifier = queryParams
                .getFirst("element_sub_identifier");
        final Set<String> sorts = getQuerySet(queryParams, "sort");
        final String tagUuidsOp = queryParams.getFirst("tag_uuids_op");

        /* Build event filter */
        final EventSummaryFilter.Builder filterBuilder = EventSummaryFilter
                .newBuilder();
        filterBuilder.addAllSeverity(severities);
        filterBuilder.addAllStatus(status);
        filterBuilder.addAllTagUuids(tagUuids);

        if (eventClass != null) {
            filterBuilder.setEventClass(eventClass);
        }
        if (firstSeen != null) {
            filterBuilder.setFirstSeen(firstSeen);
        }
        if (lastSeen != null) {
            filterBuilder.setLastSeen(lastSeen);
        }
        if (statusChange != null) {
            filterBuilder.setStatusChange(statusChange);
        }
        if (count != null) {
            filterBuilder.setCount(count);
        }
        if (summary != null) {
            filterBuilder.setEventSummary(summary);
        }
        if (element_identifier != null) {
            filterBuilder.setElementIdentifier(element_identifier);
        }
        if (element_sub_identifier != null) {
            filterBuilder.setElementSubIdentifier(element_sub_identifier);
        }
        if (tagUuidsOp != null) {
            filterBuilder.setTagUuidsOp(FilterOperator.valueOf(tagUuidsOp
                    .toUpperCase()));
        }

        final EventSummaryFilter filter = filterBuilder.build();

        /* Build event request */
        final EventSummaryRequest.Builder reqBuilder = EventSummaryRequest
                .newBuilder();
        reqBuilder.setFilter(filter);
        reqBuilder.addAllKeys(keys);

        if (limit < 0) {
            throw new IllegalArgumentException("Invalid limit: " + limit);
        }
        reqBuilder.setLimit(limit);

        if (offset < 0) {
            throw new IllegalArgumentException("Invalid offset: " + offset);
        }
        reqBuilder.setOffset(offset);

        for (String sort : sorts) {
            String[] sortParts = sort.split(",", 2);

            EventSort.Builder eventSort = EventSort.newBuilder();
            eventSort.setField(EventSort.Field.valueOf(sortParts[0]));

            if (sortParts.length == 2) {
                eventSort.setDirection(EventSort.Direction
                        .valueOf(sortParts[1]));
            }

            reqBuilder.addSort(eventSort);
        }

        return reqBuilder.build();
    }

    static TimestampRange parseRange(String range) throws ParseException {
        if (range == null) {
            return null;
        }
        final TimestampRange.Builder builder = TimestampRange.newBuilder();
        final int solidusIndex = range.indexOf('/');

        if (solidusIndex != -1) {
            final String start = range.substring(0, solidusIndex);
            final String end = range.substring(solidusIndex + 1);
            final long startTs = parseISO8601(start);
            final long endTs = parseISO8601(end);
            if (startTs > endTs) {
                throw new IllegalArgumentException(start + " > " + end);
            }
            builder.setStartTime(startTs).setEndTime(endTs);
        } else {
            builder.setStartTime(parseISO8601(range));
        }
        return builder.build();
    }

    static long parseISO8601(String str) throws ParseException {
        final SimpleDateFormat fmt;
        if (str.indexOf('.') > 0) {
            fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        } else {
            fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        }
        fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        return fmt.parse(str).getTime();
    }

    static NumberCondition convertCount(String count) {
        if (count == null) {
            return null;
        }
        final Operation op;
        final String strNum;
        switch (count.charAt(0)) {
        case '>':
            if (count.charAt(1) == '=') {
                op = Operation.GTEQ;
                strNum = count.substring(2);
            } else {
                op = Operation.GT;
                strNum = count.substring(1);
            }
            break;
        case '<':
            if (count.charAt(1) == '=') {
                op = Operation.LTEQ;
                strNum = count.substring(2);
            } else {
                op = Operation.LT;
                strNum = count.substring(1);
            }
            break;
        case '=':
            op = Operation.EQ;
            strNum = count.substring(1);
            break;
        default:
            op = Operation.EQ;
            strNum = count;
            break;
        }
        final int val = Integer.parseInt(strNum);
        if (val < 0) {
            throw new IllegalArgumentException("Invalid value - must be >= 0: "
                    + val);
        }
        return NumberCondition.newBuilder().setOp(op).setValue(val).build();
    }
}
