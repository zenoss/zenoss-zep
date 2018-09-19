package org.zenoss.zep.zing.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.pubsub.v1.ProjectTopicName;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.api.gax.core.CredentialsProvider;

import org.zenoss.zep.zing.ZingConfig;

import java.io.IOException;

public class ZingMessagePublisher {

    private static final Logger logger = LoggerFactory.getLogger(ZingEventProcessorImpl.class);

    private final Publisher publisher;

    private final ProjectTopicName topicName;

    public ZingMessagePublisher(ZingConfig config) {
        this.topicName = ProjectTopicName.of(config.project, config.topic);
        Publisher psPublisher = null;
        Publisher.Builder builder = Publisher.newBuilder(topicName);
        if (!config.credentialsPath.isEmpty()){
            CredentialsProvider credentialsProvider = this.buildCredentials(config.credentialsPath);
            if (credentialsProvider!=null) {
                builder.setCredentialsProvider(credentialsProvider);
            }
        }
        // FIXME currently getting dependency issues with com.google.protobuf
        /*
        try {
            psPublisher = builder.build();    
        } catch(IOException e) {
            logger.error("Exception creating pubsub publisher", e);
        }*/
        this.publisher = psPublisher;
    }

    private CredentialsProvider buildCredentials(String filepath) {
        return null;
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

