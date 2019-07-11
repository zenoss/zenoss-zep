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
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.zing.proto.event.Event;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Counter;


public abstract class ZingPublisher {

    private static final Logger logger = LoggerFactory.getLogger(ZingPublisher.class);

    private Publisher publisher = null;

    private ProjectTopicName topicName;

    private ZingConfig config;

    private MetricRegistry metricRegistry;

    private Counter sentEventsCounter;

    private Counter failedEventsCounter;

    private Counter bytesSentCounter;

    public ZingPublisher(MetricRegistry metrics, ZingConfig config) {
        this.topicName = ProjectTopicName.of(config.project, config.topic);
        this.config = config;
        this.metricRegistry = metrics;
        this.sentEventsCounter = this.metricRegistry.counter("zing.sentEvents");
        this.failedEventsCounter = this.metricRegistry.counter("zing.failedEvents");
        this.bytesSentCounter = this.metricRegistry.counter("zing.bytesSent");
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
        logger.debug("published with message id: " + messageId);
        this.sentEventsCounter.inc();
    }

    protected void onFailure(Throwable t) {
        logger.warn("failed to publish event to pubsub: " + t);
        this.failedEventsCounter.inc();
    }

    private String getMessageId(Event zingEvent) {
        return zingEvent.getTenant() + "-" +
                zingEvent.getName() + "-" +
                zingEvent.getId() + "-" +
                zingEvent.hashCode() + "-" +
                zingEvent.getTimestamp();
    }

    public void publishEvent(ZingEvent event) {
        if (this.publisher != null) {
            final Event zingEvent = event.toZingEventProto();
            final ByteString bytes = zingEvent.toByteString();
            this.bytesSentCounter.inc(bytes.size());
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                    .setData(bytes)
                    .putAttributes(ZingConstants.PUBSUB_ID_ATTRIBUTE, getMessageId(zingEvent))
                    .build();
            ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
            ApiFutures.addCallback(messageIdFuture, new ApiFutureCallback<String>() {
                @Override
                public void onSuccess(String messageId) {
                    ZingPublisher.this.onSuccess(messageId);
                }

                @Override
                public void onFailure(Throwable t) {
                    ZingPublisher.this.onFailure(t);
                }
            }, MoreExecutors.directExecutor());
        }
    }
}
