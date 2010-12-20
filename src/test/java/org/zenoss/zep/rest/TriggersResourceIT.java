/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2010, Zenoss Inc.
 * 
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 * 
 * For complete information please visit: http://www.zenoss.com/oss/
 */
package org.zenoss.zep.rest;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zenoss.protobufs.JsonFormat;
import org.zenoss.protobufs.zep.Zep.EventTrigger;
import org.zenoss.protobufs.zep.Zep.EventTriggerSet;
import org.zenoss.protobufs.zep.Zep.EventTriggerSubscription;
import org.zenoss.protobufs.zep.Zep.EventTriggerSubscriptionSet;
import org.zenoss.protobufs.zep.Zep.Rule;
import org.zenoss.protobufs.zep.Zep.RuleType;

public class TriggersResourceIT {

    private static final String TRIGGERS_URI = "/zenoss-zep/api/1.0/triggers";
    private RestClient client;

    @Before
    public void setup() {
        this.client = new RestClient(EventTrigger.getDefaultInstance(),
                EventTriggerSet.getDefaultInstance(),
                EventTriggerSubscriptionSet.getDefaultInstance());
    }

    @After
    public void shutdown() throws IOException {
        this.client.close();
    }

    private EventTrigger createTrigger() throws IOException {
        EventTrigger.Builder triggerBuilder = EventTrigger.newBuilder();
        triggerBuilder.setUuid(UUID.randomUUID().toString());

        Rule.Builder ruleBuilder = Rule.newBuilder();
        ruleBuilder.setApiVersion(1);
        ruleBuilder.setSource("False");
        ruleBuilder.setType(RuleType.RULE_TYPE_JYTHON);
        triggerBuilder.setRule(ruleBuilder.build());

        final EventTrigger trigger = triggerBuilder.build();

        assertEquals(
                HttpURLConnection.HTTP_CREATED,
                client.putProtobuf(TRIGGERS_URI + "/" + trigger.getUuid(),
                        trigger).getResponseCode());

        return trigger;
    }

    @Test
    public void testRest() throws IOException {
        client.getProtobuf(TRIGGERS_URI);
        client.getJson(TRIGGERS_URI);

        EventTrigger trigger = createTrigger();
        String triggerUri = TRIGGERS_URI + "/" + trigger.getUuid();

        {
            EventTrigger triggerFromRest = (EventTrigger) client.getProtobuf(
                    triggerUri).getMessage();
            assertEquals(trigger.getUuid(), triggerFromRest.getUuid());
            assertEquals(trigger.getRule(), triggerFromRest.getRule());
        }
        {
            EventTrigger triggerFromRest = (EventTrigger) client.getJson(
                    triggerUri).getMessage();
            assertEquals(trigger.getUuid(), triggerFromRest.getUuid());
            assertEquals(trigger.getRule(), triggerFromRest.getRule());
        }

        {
            EventTriggerSet set = (EventTriggerSet) client.getProtobuf(
                    TRIGGERS_URI).getMessage();
            boolean found = false;
            for (EventTrigger triggerFromRest : set.getTriggersList()) {
                if (triggerFromRest.getUuid().equals(trigger.getUuid())
                        && trigger.getRule().equals(triggerFromRest.getRule())) {
                    found = true;
                    break;
                }
            }
            assertTrue("Didn't find trigger in list of all triggers", found);
        }
        {
            EventTriggerSet set = (EventTriggerSet) client
                    .getJson(TRIGGERS_URI).getMessage();
            boolean found = false;
            for (EventTrigger triggerFromRest : set.getTriggersList()) {
                if (triggerFromRest.getUuid().equals(trigger.getUuid())
                        && trigger.getRule().equals(triggerFromRest.getRule())) {
                    found = true;
                    break;
                }
            }
            assertTrue("Didn't find trigger in list of all triggers", found);
        }
    }

    private void assertSubscriptionsEqual(EventTriggerSubscriptionSet expected,
            EventTriggerSubscriptionSet actual) throws IOException {
        int numFound = 0;
        for (EventTriggerSubscription subscription : expected
                .getSubscriptionsList()) {
            for (EventTriggerSubscription subscriptionFromRest : actual
                    .getSubscriptionsList()) {
                if (subscription.equals(EventTriggerSubscription
                        .newBuilder(subscriptionFromRest).clearUuid().build())) {
                    numFound++;
                    break;
                }
            }
        }
        assertEquals(
                String.format("EXPECTED:%n%s%nACTUAL:%n%s",
                        JsonFormat.writeAsString(expected),
                        JsonFormat.writeAsString(actual)), numFound,
                expected.getSubscriptionsCount());
    }

    @Test
    public void testSubscriptions() throws IOException {
        /* Create subscriptions to 5 triggers */
        String subscriberUuid = UUID.randomUUID().toString();
        EventTriggerSubscriptionSet.Builder subscriptionBuilder = EventTriggerSubscriptionSet
                .newBuilder();
        for (int i = 0; i < 5; i++) {
            EventTrigger trigger = createTrigger();

            EventTriggerSubscription.Builder subBuilder = EventTriggerSubscription
                    .newBuilder();
            subBuilder.setDelaySeconds(30);
            subBuilder.setRepeatSeconds(60);
            subBuilder.setSubscriberUuid(subscriberUuid);
            subBuilder.setTriggerUuid(trigger.getUuid());
            subscriptionBuilder.addSubscriptions(subBuilder.build());
        }
        String subscriptionUri = TRIGGERS_URI + "/subscriptions/"
                + subscriberUuid;
        EventTriggerSubscriptionSet subscriptions = subscriptionBuilder.build();
        assertEquals(HttpURLConnection.HTTP_NO_CONTENT,
                client.putProtobuf(subscriptionUri, subscriptions)
                        .getResponseCode());
        /* Verify all subscriptions are returned */
        assertSubscriptionsEqual(
                subscriptions,
                (EventTriggerSubscriptionSet) client.getProtobuf(
                        subscriptionUri).getMessage());
        assertSubscriptionsEqual(subscriptions,
                (EventTriggerSubscriptionSet) client.getJson(subscriptionUri)
                        .getMessage());

        /* Remove all subscriptions with an empty set */
        assertEquals(
                HttpURLConnection.HTTP_NO_CONTENT,
                client.putProtobuf(subscriptionUri,
                        EventTriggerSubscriptionSet.getDefaultInstance())
                        .getResponseCode());
        assertEquals(
                0,
                ((EventTriggerSubscriptionSet) client.getProtobuf(
                        subscriptionUri).getMessage()).getSubscriptionsCount());
        assertEquals(0,
                ((EventTriggerSubscriptionSet) client.getJson(subscriptionUri)
                        .getMessage()).getSubscriptionsCount());

        /* Create all subscriptions again */
        assertEquals(HttpURLConnection.HTTP_NO_CONTENT,
                client.putProtobuf(subscriptionUri, subscriptions)
                        .getResponseCode());
        assertSubscriptionsEqual(
                subscriptions,
                (EventTriggerSubscriptionSet) client.getProtobuf(
                        subscriptionUri).getMessage());
        assertSubscriptionsEqual(subscriptions,
                (EventTriggerSubscriptionSet) client.getJson(subscriptionUri)
                        .getMessage());

        /* Delete a few and update properties of others */
        List<EventTriggerSubscription> tempSubscriptions = new ArrayList<EventTriggerSubscription>(
                subscriptions.getSubscriptionsList());
        /* Remove a few triggers */
        tempSubscriptions.remove(1);
        tempSubscriptions.remove(2);
        /* Modify a few triggers */
        tempSubscriptions.set(1,
                EventTriggerSubscription.newBuilder(tempSubscriptions.get(1))
                        .setDelaySeconds(90).setRepeatSeconds(120).build());
        tempSubscriptions.set(2,
                EventTriggerSubscription.newBuilder(tempSubscriptions.get(2))
                        .setDelaySeconds(150).setRepeatSeconds(720).build());
        /* Add a new trigger */
        tempSubscriptions.add(EventTriggerSubscription.newBuilder()
                .setDelaySeconds(600).setRepeatSeconds(0)
                .setSubscriberUuid(subscriberUuid)
                .setTriggerUuid(createTrigger().getUuid()).build());
        subscriptions = EventTriggerSubscriptionSet.newBuilder()
                .addAllSubscriptions(tempSubscriptions).build();
        assertEquals(HttpURLConnection.HTTP_NO_CONTENT,
                client.putProtobuf(subscriptionUri, subscriptions)
                        .getResponseCode());
        assertSubscriptionsEqual(
                subscriptions,
                (EventTriggerSubscriptionSet) client.getProtobuf(
                        subscriptionUri).getMessage());
        assertSubscriptionsEqual(subscriptions,
                (EventTriggerSubscriptionSet) client.getJson(subscriptionUri)
                        .getMessage());
    }
}
