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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.ApplicationListener;
import org.zenoss.amqp.AmqpException;
import org.zenoss.amqp.Channel;
import org.zenoss.protobufs.zep.Zep.ZepRawEvent;
import org.zenoss.zep.EventProcessor;
import org.zenoss.zep.events.EventIndexQueueSizeEvent;

public class RawEventQueueListener extends AbstractQueueListener
    implements ApplicationListener<EventIndexQueueSizeEvent>, ApplicationEventPublisherAware {

    private static final Logger logger = LoggerFactory.getLogger(RawEventQueueListener.class);

    private int prefetchCount = 100;

    private EventProcessor eventProcessor;

    public void setPrefetchCount(int prefetchCount) {
        this.prefetchCount = prefetchCount;
    }
    
    private boolean throttleConsumer = true;
    private volatile boolean indexQueueLag = false;
    private int indexQueueThreshold = 10000;
    private int consumerSleepTime = 1000;

    @Override
    public void onApplicationEvent(EventIndexQueueSizeEvent event) {
        if (this.throttleConsumer && event.getTableName().startsWith("event_summary")) {
            int localThreshold = this.indexQueueThreshold;
            if (localThreshold == 0) {
                // autoset the threshold
                // event.getLimit() contains the current batch size
                localThreshold = Math.max(event.getLimit(), 100) * 2;
            }

            int size = event.getSize();

            // If already throttling, we wont resume until we are
            // 50% below the threshold
            if (this.indexQueueLag==true) {
                localThreshold = (int) (localThreshold - 0.5*localThreshold);
            }

            if (size > localThreshold) {
                // enable the throttle
                if (this.indexQueueLag != true) {
                    logger.warn("Enabling zenevents consumer throttling. Queue Size: {}.", size);
                    this.indexQueueLag = true;
                }
            } else {
                if (this.indexQueueLag != false) {
                    logger.info("Disabling zenevents consumer throttling. Queue Size: {}.", size);
                    this.indexQueueLag = false;
                }
            }
        }
    }

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
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

    private ApplicationEventPublisher applicationEventPublisher;

    public void setThrottleConsumer(boolean throttleConsumer) {
        this.throttleConsumer = throttleConsumer;
    }

    public void setIndexQueueThreshold(int indexQueueThreshold) {
        this.indexQueueThreshold = indexQueueThreshold;
    }

    public void setConsumerSleepTime(int consumerSleepTime) {
        this.consumerSleepTime = consumerSleepTime;
    }

    @Override
    public void handle(com.google.protobuf.Message message) throws Exception {
        if (!(message instanceof ZepRawEvent)) {
            logger.warn("Unexpected message type: {}", message);
        } else {
            while (this.indexQueueLag && this.throttleConsumer) {
                Thread.sleep(this.consumerSleepTime);
            }
            this.eventProcessor.processEvent((ZepRawEvent) message);
        }
    }
}
