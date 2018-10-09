/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2018, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.zing;


import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.zing.proto.event.Event;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Counter;
import com.codahale.metrics.annotation.Timed;


public abstract class ZingPublisher {

    private static final Logger logger = LoggerFactory.getLogger(ZingPublisher.class);

    private Publisher publisher = null;

    private ProjectTopicName topicName;

    private ZingConfig config;

    protected MetricRegistry metricRegistry;

    protected Counter sentEventsCounter;

    protected Counter failedEventsCounter;

    public ZingPublisher(MetricRegistry metrics, ZingConfig config) {
        this.topicName = ProjectTopicName.of(config.project, config.topic);
        this.config = config;
        this.metricRegistry = metrics;
        this.sentEventsCounter = this.metricRegistry.counter("zing.sentEvents");
        this.failedEventsCounter = this.metricRegistry.counter("zing.failedEvents");
    }

    public void setPublisher(Publisher p) {
        this.publisher = p;
    }

    public ZingConfig getConfig() {
        return config;
    }

    public void setConfig(ZingConfig config) {
        this.config = config;
    }

    public ProjectTopicName getTopicName() {
        return this.topicName;
    }

    public void shutdown() {
        if (this.publisher != null) {
            try {
                this.publisher.shutdown();
            } catch (Exception e) {
                logger.warn("Exception shutting down pubsub publisher", e);
            }
        }
    }

    protected void onSuccess(String messageId) {
        // FIXME set this to debug or remove
        logger.info("published with message id: " + messageId);
        this.sentEventsCounter.inc();
    }

    protected void onFailure(Throwable t) {
        logger.warn("failed to publish event to pubsub: " + t);
        this.failedEventsCounter.inc();
    }

    @Timed(absolute = true, name = "zing.publishEvent")
    public void publishEvent(ZingEvent event) {
        if (this.publisher != null) {
            final Event zingEvent = event.toZingEvent();
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(zingEvent.toByteString()).build();

            ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
            ApiFutures.addCallback(messageIdFuture, new ApiFutureCallback<String>() {
                public void onSuccess(String messageId) {
                    ZingPublisher.this.onSuccess(messageId);
                }
                public void onFailure(Throwable t) {
                    ZingPublisher.this.onFailure(t);
                }
            }, MoreExecutors.directExecutor());
        }
    }
}
