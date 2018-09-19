
package org.zenoss.zep.zing.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.zep.Zep.EventSummary;

import org.zenoss.zep.zing.ZingEventProcessor;
import org.zenoss.zep.zing.ZingConfig;


public class ZingEventProcessorImpl implements ZingEventProcessor {

    private static final Logger logger = LoggerFactory.getLogger(ZingEventProcessorImpl.class);

    private boolean enabled;

    private final ZingConfig config;

    private ZingMessagePublisher publisher = null;

    public ZingEventProcessorImpl(ZingConfig cfg) {
        this.config = cfg;
        logger.info("Zing Event Processor created with config: {}", cfg.toString());
        this.enabled = this.config.forwardEvents();
        if (this.enabled) {
            if (this.config.validate()) {
                this.publisher = new ZingMessagePublisher(this.config);
            } else {
                logger.error("Zing configuration is not valid. Events will not be forwarded to Zenoss Cloud");
                this.enabled = false;
            }
        }
        if(this.publisher==null) {
            this.enabled = false;
            logger.error("Could not create publisher. Events will not be forwarded to Zenoss Cloud");
        }
    }

    public void processEvent(EventSummary eventSummary) {
        if (this.enabled) {
            logger.info("PROCESS EVENTTT YOOO");
        } else {
            logger.info("DONT FORWARD EVENTS YOOO");
        }
    }

    public void shutdown() {
        logger.info("SHUTTING DOWN ZING EVENT PROCESSOR");
        if (this.enabled) {
            this.publisher.shutdown();
        }
    }
}