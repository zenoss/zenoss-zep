package org.zenoss.zep.rest;

import com.csvreader.CsvWriter;
import com.google.common.collect.Sets;
import org.jboss.resteasy.annotations.GZIP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.zenoss.protobufs.ProtobufConstants;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.protobufs.zep.Zep.EventDetail;
import org.zenoss.protobufs.zep.Zep.EventDetailFilter;
import org.zenoss.protobufs.zep.Zep.EventQuery;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventSummaryResult;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.index.EventIndexDao;
import org.zenoss.zep.index.EventIndexer;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Path("2/etl")
public class EtlResource {

    public static final Logger logger = LoggerFactory.getLogger(EtlResource.class);
    public static final int limit = 1000;

    public static class EventGetter {

        private final EventIndexDao eventIndexDao;
        private final String savedSearchUuid;
        private final String eventSourceTable;

        public EventGetter(
                final EventIndexer eventIndexer, final EventIndexDao eventIndexDao,
                final EventQuery eventQuery, final String eventSourceTable
        ) throws ZepException {
            this.eventIndexDao = eventIndexDao;
            eventIndexer.indexFully();
            this.savedSearchUuid = eventIndexDao.createSavedSearch(eventQuery);
            this.eventSourceTable = eventSourceTable;
        }

        public EventSummaryResult getEventSummaryResult(final int nextOffset) throws ZepException {
            return this.eventIndexDao.savedSearch(this.savedSearchUuid, nextOffset, limit);
        }

        public String getEventSourceTable() {
            return this.eventSourceTable;
        }

        public void deleteSavedSearch() throws ZepException {
            this.eventIndexDao.deleteSavedSearch(this.savedSearchUuid);
        }

    }

    @Autowired
    private EventIndexer eventSummaryIndexer;

    @Autowired
    private EventIndexer eventArchiveIndexer;

    @Autowired
    private EventIndexDao eventSummaryIndexDao;

    @Autowired
    private EventIndexDao eventArchiveIndexDao;

    @POST
    @Path("search")
    @Consumes({MediaType.APPLICATION_JSON, ProtobufConstants.CONTENT_TYPE_PROTOBUF})
    @Produces({MediaType.TEXT_PLAIN})
    @GZIP
    public Response getEventEtlCsv(final EventQuery eventQuery) throws ZepException {
        final EventGetter summaryGetter = new EventGetter(
                this.eventSummaryIndexer, this.eventSummaryIndexDao, eventQuery, "status"
        );
        final EventGetter archiveGetter = new EventGetter(
                this.eventArchiveIndexer, this.eventArchiveIndexDao, eventQuery, "history"
        );
        final EventGetter[] eventGetters = new EventGetter[]{summaryGetter, archiveGetter};

        final List<String> detailNames = new ArrayList<String>();
        List<EventDetailFilter> detailFilters = eventQuery.getEventFilter().getDetailsList();
        if (detailFilters != null) {
            for (EventDetailFilter df: detailFilters) {
                detailNames.add(df.getKey());
            }
        }

        StreamingOutput streamingOutput = new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                try {
                    Set<String> summaryEventIds = Sets.newHashSet();
                    CsvWriter csvWriter = new CsvWriter(new OutputStreamWriter(output), ',');
                    for (EventGetter eventGetter : eventGetters) {
                        EventSummaryResult result = EventSummaryResult.newBuilder().setNextOffset(0).build();
                        if (logger.isDebugEnabled()) {
                            logger.debug("Processing event getter for table " + eventGetter.eventSourceTable);
                        }

                        while (result.hasNextOffset()) {
                            try {
                                result = eventGetter.getEventSummaryResult(result.getNextOffset());
                                if (logger.isDebugEnabled()) {
                                    logger.debug(
                                            "Results for " + eventGetter.eventSourceTable +
                                                    " num results = " + result.getEventsCount()
                                    );
                                }
                            } catch (ZepException e) {
                                throw new RuntimeException(
                                        "Failed to use saved search for " + eventGetter.getEventSourceTable(), e
                                );
                            }
                            for (EventSummary eventSummary : result.getEventsList()) {
                                if (!summaryEventIds.contains(eventSummary.getUuid())) {
                                    RecordBuilder builder = new RecordBuilder(eventGetter, eventSummary);
                                    for (String detailName: detailNames) {
                                        builder.appendDetail(detailName);
                                    }
                                    csvWriter.writeRecord(builder.build());
                                    if (eventGetter == summaryGetter) {
                                        summaryEventIds.add(eventSummary.getUuid());
                                    }
                                }
                            }
                            csvWriter.flush();
                        }
                    }
                } finally {
                    for (EventGetter eventGetter : eventGetters) {
                        try {
                            eventGetter.deleteSavedSearch();
                        } catch (ZepException e) {
                            logger.error("Failed to delete saved search for " + eventGetter.getEventSourceTable(), e);
                        }
                    }
                }
            }
        };

        return Response.ok(streamingOutput).build();
    }

    private static class RecordBuilder {

        private EventGetter getter = null;
        private EventSummary summary = null;
        private Event occurrence = null;
        private EventActor actor = null;
        private Map<String, String> details = null;
        private ArrayList<String> record = null;

        public RecordBuilder(EventGetter getter, EventSummary summary) {
            this.getter = getter;
            this.summary = summary;

            this.occurrence = summary.getOccurrence(0);
            this.actor = occurrence.getActor();
            this.details = detailsMap(occurrence.getDetailsList());

            this.record = new ArrayList<String>();
            addDefaultColumns();
        }

        public RecordBuilder appendDetail(String detailName) {
            record.add(details.get(detailName));
            return this;
        }

        public String[] build() {
            return record.toArray(new String[0]);
        }

        private void addDefaultColumns() {
            // 0 zenoss_instance_key (e.g. 42)
            record.add(details.get("zenoss.analytics.zenoss_instance_key"));

            // 1 event_source_table (e.g. summary or archive)
            record.add(getter.getEventSourceTable());

            // 2 dedupid (e.g. localhost|eth0|/App/Print|5|brian's test event)
            record.add(occurrence.getFingerprint().replaceAll("[\r]", ""));

            // 3 evid (e.g. 000c2995-f419-913b-11e2-465894bfcc4e)
            record.add(summary.getUuid());

            // 4 device (e.g. localhost)
            record.add(actor.getElementIdentifier());

            // 5 component (e.g. eth0)
            record.add(actor.getElementSubIdentifier());

            // 6 eventClass (e.g. /App/Print)
            record.add(occurrence.getEventClass());

            // 7 eventKey
            record.add(occurrence.getEventKey());

            // 8 summary
            record.add(
                    (occurrence.getSummary() != null)
                            ? StringUtils.trimWhitespace(occurrence.getSummary()).replaceAll("[\r]", "") : null
            );

            // 9 message
            record.add(occurrence.getMessage().replaceAll("[\r]", ""));

            // 10 severity (e.g. 5)
            record.add(Integer.toString(occurrence.getSeverity().getNumber()));

            // 11 event_severity_name (e.g. Critical)
            record.add(occurrence.getSeverity().name().substring("SEVERITY_".length()).toLowerCase());

            // 12 eventState (e.g. 0)
            record.add(Integer.toString(summary.getStatus().getNumber()));

            // 13 event_state_name (e.g. New)
            record.add(summary.getStatus().name().substring("STATUS_".length()).toLowerCase());

            // 14 eventClassKey
            record.add(occurrence.getEventClassKey());

            // 15 eventGroup
            record.add(occurrence.getEventGroup());

            // 16 firstTime (e.g. 1355535704)
            record.add(Long.toString(summary.getFirstSeenTime()));

            // 17 lastTime (e.g. 1355535704)
            record.add(Long.toString(summary.getLastSeenTime()));

            // 18 count (e.g. 1)
            record.add(Integer.toString(summary.getCount()));

            // 19 prodState (e.g. 1000)
            record.add(details.get("zenoss.device.production_state"));

            // 20 agent
            record.add(occurrence.getAgent());

            // 21 monitor
            record.add(occurrence.getMonitor());

            // 22 entity_key (e.g. 711)
            record.add(details.get("zenoss.analytics.entity_key"));

            // 23 entity_class_name (e.g. device)
            record.add(details.get("zenoss.analytics.entity_class_name"));

            // 24 The UUID of the clearing event.
            record.add(summary.getClearedByEventUuid());

            // 25 last status change time
            record.add(Long.toString(summary.getStatusChangeTime()));

            // 26 actor.element_uuid so that the other analytics props can added later.
            record.add(actor.getElementUuid());
        }
    }

    private static Map<String, String> detailsMap(final List<EventDetail> detailList) {
        Map<String, String> details = new HashMap<String, String>();
        for (EventDetail detail : detailList) {
            String value = null;
            if (detail.getValueCount() > 0) {
                value = detail.getValue(0);
            }
            details.put(detail.getName(), value);
        }
        return details;
    }
}
