/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2018, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

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
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.zep.zing.ZingConfig;
import org.zenoss.zep.zing.ZingEvent;
import org.zenoss.zep.zing.ZingPublisher;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ZingEmulatorPublisherImpl extends ZingPublisher {

    private static final Logger logger = LoggerFactory.getLogger(ZingEmulatorPublisherImpl.class);

    private AtomicBoolean everConnected;

    public ZingEmulatorPublisherImpl(ZingConfig config) {
        super(config);
        logger.info("Creating Publisher to PubSub emulator");
        this.everConnected = new AtomicBoolean(false);
        this.setPublisher(this.buildPublisher(config));
    }

    private void createTopic(TransportChannelProvider channelProvider) {
        // Make sure topic exists. This is called once the undelying channel is connected
        CredentialsProvider credentialsProvider = NoCredentialsProvider.create();
        try {
            TopicAdminClient topicAdminClient = TopicAdminClient.create(
                    TopicAdminSettings.newBuilder()
                            .setTransportChannelProvider(channelProvider)
                            .setCredentialsProvider(credentialsProvider)
                            .build());
            topicAdminClient.createTopic(this.getTopicName());
            logger.info("topic {} created in emulator", this.getTopicName());
        } catch(AlreadyExistsException aee) {
            logger.info("topic {} already exists in emulator", this.getTopicName());
        } catch(IOException e) {
            logger.error("Exception creating pubsub topic on emulator", e);
        }
    }

    // When starting zep, if we can't connect to the emulator, calling create topic
    // blocks and zep does not start properly. This method waits until the connection
    // is ready to create the topic.
    private void waitUntilConnectionReadyToCreateTopic(final ManagedChannel channel, final TransportChannelProvider channelProvider) {
        Runnable notif = new Runnable() {
            @Override public void run() {
                ConnectivityState state = channel.getState(false);
                logger.info("Connection state to emulator changed: {}", state);
                if (state != ConnectivityState.READY) {
                    // Keep waiting until connection is ready
                    channel.notifyWhenStateChanged(state, this);
                } else {
                    everConnected.set(true);
                    ZingEmulatorPublisherImpl.this.createTopic(channelProvider);
                }
            }
        };
        notif.run();
        channel.getState(true); // Force using the channel to ensure we have a working connection
    }

    private Publisher buildPublisher(ZingConfig config) {
        final ManagedChannel channel = ManagedChannelBuilder
                .forTarget(config.emulatorHostAndPort)
                .usePlaintext()
                .build();
        final TransportChannelProvider channelProvider = FixedTransportChannelProvider
                .create(GrpcTransportChannel.create(channel));
        // waitUntilConnectionReadyToCreateTopic does not block
        this.waitUntilConnectionReadyToCreateTopic(channel, channelProvider);
        Publisher publisher = null;
        try {
            publisher = Publisher.newBuilder(this.getTopicName())
                    .setChannelProvider(channelProvider)
                    .setCredentialsProvider(NoCredentialsProvider.create())
                    .build();
            logger.info("emulator publisher created");
        } catch(IOException e) {
            logger.error("Exception creating pubsub emulator publisher", e);
        }
        return publisher;
    }

    protected void onFailure(Throwable t)
    {
        logger.info("failed to publish to pubsub emulator: " + t);
    }

    public void publishEvent(ZingEvent event) {
        if (this.everConnected.get()) {
            super.publishEvent(event);
        } else {
            logger.warn("Have not been able to connect to emulator yet. Dropping event.");
        }
    }
}
