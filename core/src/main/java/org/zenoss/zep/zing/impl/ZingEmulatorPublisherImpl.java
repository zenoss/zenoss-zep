package org.zenoss.zep.zing.impl;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.ProjectTopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.zep.zing.ZingConfig;

import java.io.IOException;

public class ZingEmulatorPublisherImpl extends ZingPublisher {

    private static final Logger logger = LoggerFactory.getLogger(ZingEmulatorPublisherImpl.class);

    public ZingEmulatorPublisherImpl(ZingConfig config) {
        this.topicName = ProjectTopicName.of(config.project, config.topic);
        this.config = config;
        this.publisher = this.buildPublisher(config);
    }

    private Publisher buildPublisher(ZingConfig config) {
        String hostport = config.emulatorHostAndPort;
        ManagedChannel channel = ManagedChannelBuilder.forTarget(hostport).usePlaintext().build();
        TransportChannelProvider channelProvider =
                FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        CredentialsProvider credentialsProvider = NoCredentialsProvider.create();
        // Make sure topic exists
        try {
            TopicAdminClient topicAdminClient = TopicAdminClient.create(
                    TopicAdminSettings.newBuilder()
                            .setTransportChannelProvider(channelProvider)
                            .setCredentialsProvider(credentialsProvider)
                            .build());
            topicAdminClient.createTopic(this.topicName);
            logger.info("topic {} created in emulator", this.topicName);
        } catch(AlreadyExistsException aee) {
            logger.info("topic {} already exists in emulator", this.topicName);
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
}
