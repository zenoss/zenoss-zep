package org.zenoss.zep.zing.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.pubsub.v1.ProjectTopicName;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;

import org.zenoss.zep.zing.ZingConfig;

import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;


public class ZingPublisherImpl extends ZingPublisher {

    private static final Logger logger = LoggerFactory.getLogger(ZingPublisherImpl.class);

    public ZingPublisherImpl(ZingConfig config) {
        this.topicName = ProjectTopicName.of(config.project, config.topic);
        this.config = config;
        this.publisher = this.buildPublisher(config);
    }

    private Publisher buildPublisher(ZingConfig config) {
        Publisher psPublisher = null;
        Publisher.Builder builder = Publisher.newBuilder(topicName);
        if (!config.credentialsPath.isEmpty()){
            CredentialsProvider credentialsProvider = this.buildCredentials(config.credentialsPath);
            if (credentialsProvider!=null) {
                builder.setCredentialsProvider(credentialsProvider);
            }
        }
        try {
            psPublisher = builder.build();    
        } catch(IOException e) {
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
}

