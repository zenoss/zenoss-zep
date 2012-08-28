/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.amqp.AmqpException;
import org.zenoss.amqp.Channel;
import org.zenoss.protobufs.zep.Zep.ZepRawEvent;
import org.zenoss.zep.EventProcessor;

public class RawEventQueueListener extends AbstractQueueListener {

    private static final Logger logger = LoggerFactory.getLogger(RawEventQueueListener.class);

    private int prefetchCount = 100;

    private EventProcessor eventProcessor;

    public void setPrefetchCount(int prefetchCount) {
        this.prefetchCount = prefetchCount;
    }
    
    @Override
    protected void configureChannel(Channel channel) throws AmqpException {
        logger.debug("Using prefetch count: {} for queue: {}", this.prefetchCount, getQueueIdentifier());
        channel.setQos(0, this.prefetchCount);
    }

    @Override
    protected String getQueueIdentifier() {
        return "$ZepZenEvents";
    }

    public void setEventProcessor(EventProcessor eventProcessor) {
        this.eventProcessor = eventProcessor;
    }

    @Override
    public void handle(com.google.protobuf.Message message) throws Exception {
        if (!(message instanceof ZepRawEvent)) {
            logger.warn("Unexpected message type: {}", message);
        } else {
            this.eventProcessor.processEvent((ZepRawEvent) message);
        }
    }
}
