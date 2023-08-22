/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2018, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.zing;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.zing.proto.event.Event;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.PublisherInterface;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;


public abstract class ZingPublisher {

    private static final Logger logger = LoggerFactory.getLogger(ZingPublisher.class);

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

    public ZingConfig getConfig() {
        return config;
    }

    public void setConfig(ZingConfig config) {
        this.config = config;
    }

    public ProjectTopicName getTopicName() {
        return this.topicName;
    }

    public abstract void shutdown();

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
                zingEvent.getDimensionsMap().hashCode() + "-" +
                zingEvent.getMetadataMap().hashCode() + "-" +
                zingEvent.getTimestamp();
    }

    public void incFailedEventsCounter() {
        this.failedEventsCounter.inc();
    }

    public PubsubMessage getPubSubMessage(ZingEvent event) {
        final Event zingEvent = event.toZingEventProto();
        final ByteString bytes = zingEvent.toByteString();
        return PubsubMessage.newBuilder()
            .setData(bytes)
            .putAttributes(ZingConstants.PUBSUB_ID_ATTRIBUTE, getMessageId(zingEvent))
            .build();
    }    

    public abstract PublisherInterface getPublisher();

    public void publishEvent(ZingEvent event) {
        PublisherInterface publisher = this.getPublisher();

        if (publisher != null) {
            PubsubMessage pubsubMessage = getPubSubMessage(event);
            this.bytesSentCounter.inc(pubsubMessage.getData().size());
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

    protected CredentialsProvider buildCredentials(String filepath) {
        CredentialsProvider credentialsProvider = null;
        try {
            credentialsProvider =
                FixedCredentialsProvider.create(
                    ServiceAccountCredentials.fromStream(new FileInputStream(filepath)));
        } catch (FileNotFoundException fe) {
            logger.error("Could not open credentials file {}", filepath);
        } catch (IOException e) {
            logger.error("Exception creating pubsub credentials from file {} / {}", filepath, e);
        }
        return credentialsProvider;
    }
}
