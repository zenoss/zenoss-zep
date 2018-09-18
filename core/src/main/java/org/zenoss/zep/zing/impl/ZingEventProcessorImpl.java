
package org.zenoss.zep.zing.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.zep.Zep.EventSummary;

import org.zenoss.zep.zing.ZingEventProcessor;
import org.zenoss.zep.zing.ZingConfig;

public class ZingEventProcessorImpl implements ZingEventProcessor {

    private static final Logger logger = LoggerFactory.getLogger(ZingEventProcessorImpl.class);

    private final ZingConfig config;

    public ZingEventProcessorImpl(ZingConfig cfg) {
        this.config = cfg;
        logger.info("Zing Event Processor Created: {}", cfg.toString());
    }

    public void processEvent(EventSummary eventSummary) {
        logger.info("PROCESS EVENTTT YOOO");
    }
}