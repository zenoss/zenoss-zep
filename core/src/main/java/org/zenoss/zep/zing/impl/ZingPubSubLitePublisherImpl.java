/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2023, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.zing.impl;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.zep.zing.ZingConfig;
import org.zenoss.zep.zing.ZingPublisher;

import com.codahale.metrics.MetricRegistry;
import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.pubsub.v1.PublisherInterface;
import com.google.cloud.pubsublite.CloudRegionOrZone;
import com.google.cloud.pubsublite.ProjectNumber;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.cloudpubsub.Publisher;
import com.google.cloud.pubsublite.cloudpubsub.PublisherSettings;

public class ZingPubSubLitePublisherImpl extends ZingPublisher {

    private static final Logger logger = LoggerFactory.getLogger(ZingPubSubLitePublisherImpl.class);

    private Publisher publisher = null;

    public ZingPubSubLitePublisherImpl(MetricRegistry metrics, ZingConfig config) {
        super(metrics, config);
        logger.info("Creating Publisher to PubSubLite");
        this.setPublisher(this.buildPublisher(config));
    }

    public PublisherInterface getPublisher() {
        return this.publisher;
    }

    public void setPublisher(Publisher p) {
        this.publisher = p;
    }

    private Publisher buildPublisher(ZingConfig config) {
        CloudRegionOrZone location = CloudRegionOrZone.parse(config.pubsubLiteLocation);

        TopicPath topicPath = TopicPath.newBuilder()
            .setProject(ProjectNumber.of(config.pubsubLiteProjectNumber))
            .setLocation(location)
            .setName(TopicName.of(config.topic))
            .build();

        Publisher publisher = null;

        try {
            PublisherSettings.Builder builder = PublisherSettings.newBuilder().setTopicPath(topicPath);
            if (!config.credentialsPath.isEmpty()){
                CredentialsProvider credentialsProvider = this.buildCredentials(config.credentialsPath);
                if (credentialsProvider!=null) {
                    builder.setCredentialsProvider(credentialsProvider);
                }
            }

            PublisherSettings publisherSettings = builder.build();
            publisher = Publisher.create(publisherSettings);

            logger.info("pubsub lite publisher created (topicPath={}), starting it..", topicPath);
            publisher.startAsync().awaitRunning(120L, TimeUnit.SECONDS);
            logger.info("pubsub lite publisher started, state={}", publisher.state());

        } catch(Exception e) {
            logger.error("Exception creating pubsub lite publisher", e);
            return null;
        }

        return publisher;
    }

    public void shutdown() {
        if (this.publisher != null) {
            try {
                publisher.stopAsync().awaitTerminated(60L, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.warn("Exception shutting down pubsublite publisher", e);
            }
        }
    }
}
