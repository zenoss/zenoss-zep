/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2018, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.zing.impl;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.zep.zing.ZingConfig;
import org.zenoss.zep.zing.ZingPublisher;

import com.codahale.metrics.MetricRegistry;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.PublisherInterface;

public class ZingPublisherImpl extends ZingPublisher {

    private static final Logger logger = LoggerFactory.getLogger(ZingPublisherImpl.class);

    private Publisher publisher = null;

    public ZingPublisherImpl(MetricRegistry metrics, ZingConfig config) {
        super(metrics, config);
        logger.info("Creating Publisher to GCP PubSub");
        this.setPublisher(this.buildPublisher(config));
    }

    public PublisherInterface getPublisher() {
        return this.publisher;
    }

    public void setPublisher(Publisher p) {
        this.publisher = p;
    }

    private Publisher buildPublisher(ZingConfig config) {
        Publisher psPublisher = null;
        Publisher.Builder builder = Publisher.newBuilder(this.getTopicName());
        if (!config.credentialsPath.isEmpty()){
            CredentialsProvider credentialsProvider = this.buildCredentials(config.credentialsPath);
            if (credentialsProvider!=null) {
                builder.setCredentialsProvider(credentialsProvider);
            }
        }
        try {
            psPublisher = builder.build();    
        } catch(IOException e) {
            psPublisher = null;
            logger.error("Exception creating pubsub publisher", e);
        }
        return psPublisher;
    }

    protected void onFailure(Throwable t)
    {
        super.onFailure(t);
        // FIXME we need to store data somewhere to ensure zero data loss
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
}

