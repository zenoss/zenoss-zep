/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2018, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.zing.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;

import org.zenoss.zep.zing.ZingConfig;
import org.zenoss.zep.zing.ZingPublisher;

import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;


public class ZingPublisherImpl extends ZingPublisher {

    private static final Logger logger = LoggerFactory.getLogger(ZingPublisherImpl.class);

    public ZingPublisherImpl(ZingConfig config) {
        super(config);
        logger.info("Creating Publisher to GCP PubSub");
        this.setPublisher(this.buildPublisher(config));
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

    CredentialsProvider buildCredentials(String filepath) {
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

    protected void onFailure(Throwable t)
    {
        // FIXME we need to store data somewhere to ensure zero data loss
        logger.info("failed to publish to pubsub in GCP: " + t);
    }
}

