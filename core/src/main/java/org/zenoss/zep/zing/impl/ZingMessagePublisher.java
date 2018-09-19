package org.zenoss.zep.zing.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.pubsub.v1.ProjectTopicName;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.api.gax.core.CredentialsProvider;

import org.zenoss.zep.zing.ZingConfig;

import java.io.IOException;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;

public class ZingMessagePublisher {

    private static final Logger logger = LoggerFactory.getLogger(ZingEventProcessorImpl.class);

    private final Publisher publisher;

    private final ProjectTopicName topicName;

    public ZingMessagePublisher(ZingConfig config) {
        this.topicName = ProjectTopicName.of(config.project, config.topic);
        Publisher psPublisher = null;
        if (!config.useEmulator) {
            logger.info("Creating Pubsub publisher");
            psPublisher = this.buildPublisher(config);
        } else {
            //System.setenv("PUBSUB_EMULATOR_HOST", config.emulatorHostAndPort);
            logger.info("Creating Pubsub publisher for emulator");
            psPublisher = this.buildEmulatorPublisher(config);
        }
        this.publisher = psPublisher;
    }

    Publisher buildPublisher(ZingConfig config) {
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

    Publisher buildEmulatorPublisher(ZingConfig config) {
        String hostport = config.emulatorHostAndPort;
        ManagedChannel channel = ManagedChannelBuilder.forTarget(hostport).usePlaintext(true).build();
        TransportChannelProvider channelProvider =
          FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        CredentialsProvider credentialsProvider = NoCredentialsProvider.create();
        // Make sure topic exists
        // FIXME if topic exists this throws exception
        try {
            TopicAdminClient topicAdminClient = TopicAdminClient.create(
                            TopicAdminSettings.newBuilder()
                                .setTransportChannelProvider(channelProvider)
                                .setCredentialsProvider(credentialsProvider)
                                .build());
            topicAdminClient.createTopic(this.topicName);
            logger.info("topic created in emulator");
        } catch(IOException e) {
            logger.error("Exception creating pubsub topic on emulator", e);
        }
        // Create publisher
        Publisher publisher = null;
        try {
            publisher = Publisher.newBuilder(this.topicName)
                .setChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build();
            logger.info("emulator publisher created");
        } catch(IOException e) {
            logger.error("Exception creating pubsub emulator publisher", e);
        }
        return publisher;
    }

    CredentialsProvider buildCredentials(String filepath) {
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

    public void publishEvent() {
        logger.info("PACOO send msg");
    }
}

