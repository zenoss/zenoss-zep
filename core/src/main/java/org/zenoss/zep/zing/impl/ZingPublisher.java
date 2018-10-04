package org.zenoss.zep.zing.impl;


import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.zep.zing.ZingConfig;
import org.zenoss.zep.zing.ZingEvent;
import org.zenoss.zing.proto.event.Event;

public abstract class ZingPublisher {

    private static final Logger logger = LoggerFactory.getLogger(ZingPublisher.class);

    Publisher publisher = null;

    ProjectTopicName topicName;

    ZingConfig config;

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
        // FIXME set this to debug
        logger.info("published with message id: " + messageId);
    }

    protected abstract void onFailure(Throwable t);

    public void publishEvent(ZingEvent event) {
        if (this.publisher != null) {
            final Event zingEvent = event.toZingEvent();
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(zingEvent.toByteString()).build();

            ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
            ApiFutures.addCallback(messageIdFuture, new ApiFutureCallback<String>() {
                public void onSuccess(String messageId) {
                    ZingPublisher.this.onSuccess(messageId);
                }
                public void onFailure(Throwable t) {
                    ZingPublisher.this.onFailure(t);
                }
            }, MoreExecutors.directExecutor());
        }
    }
}
