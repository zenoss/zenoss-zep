/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2024, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.zing.impl;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.zep.zing.ZingConfig;
import org.zenoss.zep.zing.ZingConstants;
import org.zenoss.zep.zing.ZingEvent;
import org.zenoss.zep.zing.ZingPublisher;
import org.zenoss.zing.proto.event.Event;

import com.codahale.metrics.MetricRegistry;
import com.google.cloud.pubsub.v1.PublisherInterface;

public class ZingKafkaPublisherImpl extends ZingPublisher {

    private static final Logger logger = LoggerFactory.getLogger(ZingKafkaPublisherImpl.class);

    private KafkaProducer<String, byte[]> producer;
    private final String topic;

    public ZingKafkaPublisherImpl(MetricRegistry metrics, ZingConfig config) {
        super(metrics, config);
        this.topic = config.topic; // Reuse topic from config
        logger.info(
            "creating publisher to Kafka: topic={}, bootstrapServers={}, propertiesPath={}",
            config.topic,
            config.kafkaBootstrapServers,
            config.kafkaProducerPropertiesPath);

        this.producer = this.buildProducer(config);
    }

    @Override
    public PublisherInterface getPublisher() {
        return null;
    }

    private KafkaProducer<String, byte[]> buildProducer(ZingConfig config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put("sasl.mechanism", "OAUTHBEARER");
        props.put("sasl.login.callback.handler.class", "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");

        // Load additional properties if provided
        if (config.kafkaProducerPropertiesPath != null && !config.kafkaProducerPropertiesPath.isEmpty()) {
            try (FileInputStream in = new FileInputStream(config.kafkaProducerPropertiesPath)) {
                props.load(in);
            } catch (IOException e) {
                logger.error(
                    "failed to load Kafka producer properties from {}",
                    config.kafkaProducerPropertiesPath, e);
            }
        }

        return new KafkaProducer<>(props);
    }

    private String getMessageKey(Event zingEventProto) {
        // Similar to PubSub ID attribute, or use something else as key
        return zingEventProto.getTenant() + "-" +
                zingEventProto.getDimensionsMap().hashCode() + "-" +
                zingEventProto.getMetadataMap().hashCode() + "-" +
                zingEventProto.getTimestamp();
    }

    @Override
    public void publishEvent(ZingEvent event) {
        if (this.producer != null) {
            final Event zingEventProto = event.toZingEventProto();
            final byte[] bytes = zingEventProto.toByteArray();

            // ZingConstants.PUBSUB_ID_ATTRIBUTE used in PubSub for deduplication/ID.
            // Kafka uses key for partitioning.
            // We can use the same logic for key.
            String key = getMessageKey(zingEventProto);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(this.topic, key, bytes);

            // Add headers if needed? ZingEvent doesn't seem to have headers we need to propagate except maybe trace context?
            // PubSub implementation adds "zing-id" attribute.
            // We can add it to headers.
            record.headers().add(ZingConstants.PUBSUB_ID_ATTRIBUTE, key.getBytes());

            this.incBytesSent(bytes.length);

            this.producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        ZingKafkaPublisherImpl.this.onSuccess(key); // Passing key as messageId
                    } else {
                        ZingKafkaPublisherImpl.this.onFailure(exception);
                    }
                }
            });
        }
    }

    @Override
    public void shutdown() {
        if (this.producer != null) {
            logger.info("Closing Kafka producer");
            try {
                this.producer.close(java.time.Duration.ofSeconds(60));
            } catch (Exception e) {
                logger.warn("Exception closing Kafka producer", e);
            }
        }
    }
}
