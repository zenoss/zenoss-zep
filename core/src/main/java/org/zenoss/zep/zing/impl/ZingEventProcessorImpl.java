/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2018, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.zing.impl;

import com.codahale.metrics.Counter;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventDetail;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.protobufs.zep.Zep.EventSeverity;

import org.zenoss.zep.zing.ZingEventProcessor;
import org.zenoss.zep.zing.ZingConfig;
import org.zenoss.zep.zing.ZingPublisher;
import org.zenoss.zep.zing.ZingUtils;
import org.zenoss.zep.zing.ZingEvent;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Timed;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

public class ZingEventProcessorImpl implements ZingEventProcessor {

    private static final Logger logger = LoggerFactory.getLogger(ZingEventProcessorImpl.class);

    @Autowired
    protected MetricRegistry metricRegistry;

    protected Counter invalidEventsCounter;

    protected Counter irrelevantSeverityCounter;

    private boolean enabled;

    private final ZingConfig config;

    private ZingPublisher publisher = null;

    private EventSeverity minSeverity = EventSeverity.SEVERITY_CLEAR;

    private Integer maxPubsubMessageSize = 7*1024*1024;
    private Integer maxEventFieldLength = 50*1024;

    public ZingEventProcessorImpl(ZingConfig cfg) {
        cfg.setDefaults();

        this.config = cfg;
        logger.info("Zing Event Processor created with config: {}", cfg.toString());
        if (cfg.maxPubsubMessageSize != null) {
            this.maxPubsubMessageSize = cfg.maxPubsubMessageSize;
        }
        if (cfg.maxEventFieldLength != null) {
            this.maxEventFieldLength = cfg.maxEventFieldLength;
        }
        this.enabled = this.config.forwardEvents();
        if (this.enabled) {
            if (!this.config.validate()) {
                logger.error("Zing configuration is not valid. Events will not be forwarded to Zenoss Cloud");
                // FIXME stop zep if not running with emulator
                this.enabled = false;
            } else {
                this.minSeverity = this.parseMinSeverity(cfg.minimumSeverity);
                if (this.minSeverity == EventSeverity.SEVERITY_CLEAR) {
                    logger.info("All event severities will be forwarded to Zenoss Cloud");
                } else {
                    logger.info("Only events with severity >= {} will be forwarded to Zenoss Cloud", this.minSeverity.name());
                }
            }
        }
    }

    private EventSeverity parseMinSeverity(String input) {
        EventSeverity sev = EventSeverity.SEVERITY_CLEAR;
        if (ZingUtils.isNullOrEmpty(input)) {
            return sev;
        }
        switch (ZingUtils.sanitizeToken(input)) {
            case "CLEAR":
                sev = EventSeverity.SEVERITY_CLEAR;
                break;
            case "DEBUG":
                sev = EventSeverity.SEVERITY_DEBUG;
                break;
            case "INFO":
                sev = EventSeverity.SEVERITY_INFO;
                break;
            case "WARNING":
                sev = EventSeverity.SEVERITY_WARNING;
                break;
            case "ERROR":
                sev = EventSeverity.SEVERITY_ERROR;
                break;
            case "CRITICAL":
                sev = EventSeverity.SEVERITY_CRITICAL;
                break;
            default:
                logger.warn("Could not parse minimum severity. All events will be forwarded.");
        }
        return sev;
    }

    public void init() {
        this.invalidEventsCounter = metricRegistry.counter("zing.invalidEvents");
        this.irrelevantSeverityCounter = metricRegistry.counter("zing.irrelevantSeverityEvents");
        if (this.enabled) {
            logger.info("initializing zing event processor...");
            if (this.config.useEmulator) {
                this.publisher = new ZingEmulatorPublisherImpl(this.metricRegistry, this.config);
            } else if (this.config.usePubsubLite) {
                this.publisher = new ZingPubSubLitePublisherImpl(this.metricRegistry, this.config);
            } else {
                this.publisher = new ZingPublisherImpl(this.metricRegistry, this.config);
            }
        }
        if(this.publisher==null) {
            // FIXME stop zep if not running with emulator
            this.enabled = false;
            logger.error("Could not create publisher. Events will not be forwarded to Zenoss Cloud");
        }
    }

    public boolean enabled() {
        return this.enabled;
    }

    private void processEventSummary(EventSummary summary) {
        // build ZingEvent, convert it to protobuf and send it
        final Event event = summary.getOccurrence(0);
        if (!event.hasCreatedTime())
            return;
        ZingEvent.Builder builder = new ZingEvent.Builder(this.config.tenant,
                this.config.source,
                event.getCreatedTime());
        String uuid = "";
        if (summary != null && summary.hasUuid()) {
            uuid = summary.getUuid();
        } else if (event.hasUuid()) {
            uuid = event.getUuid();
        }
        builder.setUuid(uuid);
        if (event.hasFingerprint()) builder.setFingerprint(event.getFingerprint());
        if (event.hasSeverity()) builder.setSeverity(event.getSeverity().name());
        EventActor actor = event.getActor();
        if (actor != null) {
            if (actor.hasElementUuid()) builder.setParentContextUUID(actor.getElementUuid());
            if (actor.hasElementIdentifier()) builder.setParentContextIdentifier(actor.getElementIdentifier());
            if (actor.hasElementTitle()) {
                String parentContextTitle = actor.getElementTitle();
                Integer parentContextTitleSize = parentContextTitle.getBytes().length;
                if (parentContextTitleSize > this.maxEventFieldLength) {
                    logger.warn("event: {}; parentContextTitle is too large, trimming", uuid);
                    parentContextTitle = parentContextTitle.substring(0, this.maxEventFieldLength);
                }
                builder.setParentContextTitle(parentContextTitle);
            }
            if (actor.hasElementTypeId()) builder.setParentContextType(actor.getElementTypeId().name());
            if (actor.hasElementSubUuid()) builder.setContextUUID(actor.getElementSubUuid());
            if (actor.hasElementSubIdentifier()) builder.setContextIdentifier(actor.getElementSubIdentifier());
            if (actor.hasElementSubTitle()) {
                String contextTitle = actor.getElementSubTitle();
                Integer contextTitleSize = contextTitle.getBytes().length;
                if (contextTitleSize > this.maxEventFieldLength) {
                    logger.warn("event: {}; contextTitleSize is too large, trimming", uuid);
                    contextTitle = contextTitle.substring(0, this.maxEventFieldLength);
                }
                builder.setContextTitle(contextTitle);
            }
            if (actor.hasElementSubTypeId()) builder.setContextType(actor.getElementSubTypeId().name());
        }
        if (event.hasMessage()) {
            String message = event.getMessage();
            Integer messageSize = message.getBytes().length;
            if (messageSize > this.maxEventFieldLength) {
                logger.warn("event: {}; message is too large, trimming", uuid);
                message = message.substring(0, this.maxEventFieldLength);
            }
            builder.setMessage(message);
        }
        if (event.hasSummary()) {
            String summ = event.getSummary();
            Integer summarySize = summ.getBytes().length;
            if (summarySize > this.maxEventFieldLength) {
                summ = summ.substring(0, this.maxEventFieldLength);
                logger.warn("event: {}; summary is too large, trimming", uuid);
            }
            builder.setSummary(summ);
        }
        if (event.hasMonitor()) builder.setMonitor(event.getMonitor());
        if (event.hasAgent()) builder.setAgent(event.getAgent());
        if (event.hasEventKey()) builder.setEventKey(event.getEventKey());
        if (event.hasEventClass()) builder.setEventClass(event.getEventClass());
        if (event.hasEventClassKey()) builder.setEventClassKey(event.getEventClassKey());
        if (event.hasEventClassMappingUuid()) builder.setEventClassMappingUuid(event.getEventClassMappingUuid());
        if (event.hasEventGroup()) builder.setEventGroup(event.getEventGroup());
        if (summary.hasCount()) builder.setCount(summary.getCount());
        if (summary.hasFirstSeenTime()) builder.setFirstSeen(summary.getFirstSeenTime());
        if (summary.hasLastSeenTime()) builder.setLastSeen(summary.getLastSeenTime());
        if (summary.hasUpdateTime()) builder.setUpdateTime(summary.getUpdateTime());
        if (summary.hasStatus()) builder.setStatus(summary.getStatus().name());
        if (summary.hasClearedByEventUuid()) builder.setClearedByUUID(summary.getClearedByEventUuid());
        for (EventDetail d : event.getDetailsList()) {
            List<String> valueList = d.getValueList();
            String detailString = valueList.get(0);
            Integer detailStringSize = detailString.getBytes().length;
            if (detailStringSize > this.maxEventFieldLength) {
                logger.warn("event: {}; detail string {} is too large, trimming", uuid, d.getName());
                List<String> newValueList = new ArrayList<String>();
                newValueList.add(detailString.substring(0, this.maxEventFieldLength));
                builder.setDetail(d.getName(), newValueList);
            } else {
                builder.setDetail(d.getName(), valueList);
            }
        }

        ZingEvent zingEvent = builder.build();

        if (zingEvent.isValid()) {
            PubsubMessage pubsubMessage = this.publisher.getPubSubMessage(zingEvent);
            int pubsubMessageSize = pubsubMessage.getData().size();
            if (pubsubMessageSize >= this.maxPubsubMessageSize) {
                logger.warn("event {} is too large, dropping", zingEvent.getUuid());
                this.publisher.incFailedEventsCounter();
            } else {
                logger.debug("publishing event {}", zingEvent);
                this.publisher.publishEvent(zingEvent);
            }
        } else {
            logger.warn("dropping invalid event: {} / {}", zingEvent.getFingerprint(), zingEvent.getUuid());
            this.invalidEventsCounter.inc();
        }
    }

    @Timed(absolute=true, name="zing.processEvent")
    public void processEvent(EventSummary summary) {
        if (this.enabled) {
            EventSeverity sev = summary.getOccurrence(0).getSeverity();
            if (sev.compareTo(this.minSeverity) >= 0 ) {
                this.processEventSummary(summary);
            } else {
                this.irrelevantSeverityCounter.inc();
            }
        }
    }

    public void shutdown() {
        logger.info("Shutting down Zing Event Processor");
        if (this.enabled) {
            this.publisher.shutdown();
        }
    }
}
