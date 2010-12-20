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
package org.zenoss.zep.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.python.core.PyDictionary;
import org.python.core.PyException;
import org.python.core.PyFunction;
import org.python.core.PyInteger;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.util.PythonInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.amqp.AmqpConnectionManager;
import org.zenoss.amqp.AmqpException;
import org.zenoss.amqp.ExchangeConfiguration;
import org.zenoss.amqp.ZenossQueueConfig;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.protobufs.zep.Zep.EventDetail;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventTrigger;
import org.zenoss.protobufs.zep.Zep.EventTriggerSubscription;
import org.zenoss.protobufs.zep.Zep.Rule;
import org.zenoss.protobufs.zep.Zep.Signal;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.ZepUtils;
import org.zenoss.zep.dao.EventSignalSpool;
import org.zenoss.zep.dao.EventSignalSpoolDao;
import org.zenoss.zep.dao.EventSummaryDao;
import org.zenoss.zep.dao.EventTriggerDao;
import org.zenoss.zep.dao.EventTriggerSubscriptionDao;

import com.google.protobuf.Descriptors;

public class TriggerPlugin extends AbstractPostProcessingPlugin {

    private static final Logger logger = LoggerFactory
            .getLogger(TriggerPlugin.class);

    private EventTriggerDao triggerDao;
    private EventSignalSpoolDao signalSpoolDao;
    private EventSummaryDao eventSummaryDao;
    private EventTriggerSubscriptionDao eventTriggerSubscriptionDao;

    private AmqpConnectionManager connectionManager;

    protected PythonInterpreter python;
    protected PyFunction toObject;
    protected static final Map<EventStatus, PyString> statusAsPyString = new EnumMap<EventStatus, PyString>(
            EventStatus.class);
    protected static final Map<EventSeverity, PyString> severityAsPyString = new EnumMap<EventSeverity, PyString>(
            EventSeverity.class);
    protected static final Map<String, Class<?>> fieldPyclassMap = new HashMap<String, Class<?>>();
    protected static final int MAX_RULE_CACHE_SIZE = 200;
    protected static final Map<String, PyFunction> ruleFunctionCache = Collections
            .synchronizedMap(ZepUtils
                    .<String, PyFunction> createBoundedMap(MAX_RULE_CACHE_SIZE));

    // The maximum amount of time to wait between processing the signal spool.
    private static final int MAXIMUM_DELAY_SECONDS = 60;

    private DelayQueue<SpoolDelayed> delayed = new DelayQueue<SpoolDelayed>();
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    static {
        PythonInterpreter.initialize(System.getProperties(), new Properties(), new String[0]);
        
        statusAsPyString.put(EventStatus.STATUS_NEW, new PyString("new"));
        statusAsPyString.put(EventStatus.STATUS_ACKNOWLEDGED, new PyString("ack"));
        statusAsPyString.put(EventStatus.STATUS_SUPPRESSED, new PyString("suppressed"));
        statusAsPyString.put(EventStatus.STATUS_CLEARED, new PyString("cleared"));
        statusAsPyString.put(EventStatus.STATUS_CLOSED, new PyString("closed"));
        statusAsPyString.put(EventStatus.STATUS_DROPPED, new PyString("dropped"));
        statusAsPyString.put(EventStatus.STATUS_AGED, new PyString("aged"));

        severityAsPyString.put(EventSeverity.SEVERITY_CLEAR, new PyString("clear"));
        severityAsPyString.put(EventSeverity.SEVERITY_DEBUG, new PyString("debug"));
        severityAsPyString.put(EventSeverity.SEVERITY_INFO, new PyString("info"));
        severityAsPyString.put(EventSeverity.SEVERITY_WARNING, new PyString("warning"));
        severityAsPyString.put(EventSeverity.SEVERITY_ERROR, new PyString("error"));
        severityAsPyString.put(EventSeverity.SEVERITY_CRITICAL, new PyString("critical"));

        fieldPyclassMap.put("summary", PyString.class);
        fieldPyclassMap.put("message", PyString.class);
        fieldPyclassMap.put("event_class", PyString.class);
        fieldPyclassMap.put("fingerprint", PyString.class);
        fieldPyclassMap.put("event_key", PyString.class);
        fieldPyclassMap.put("agent", PyString.class);
        fieldPyclassMap.put("monitor", PyString.class);
    }

    public TriggerPlugin() {
    }

    @Override
    public void init(Map<String, String> properties) {
        super.init(properties);

        this.python = new PythonInterpreter();

        // define basic infrastructure class for evaluating rules
        this.python.exec(
            "class DictAsObj(object):\n" +
            "  def __init__(self, **kwargs):\n" +
            "    for k,v in kwargs.items(): setattr(self,k,v)");

        // expose to Java a Python dict->DictAsObj conversion function
        this.toObject = (PyFunction)this.python.eval("lambda dd : DictAsObj(**dd)");

        // import some helpful modules from the standard lib
        this.python.exec("import string, re, datetime");

        this.executor.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                while (true) {
                    int nextTime = processSpool();
                    if (nextTime > 0 && nextTime < MAXIMUM_DELAY_SECONDS) {
                        delayed.put(new SpoolDelayed(nextTime));
                    }
                    delayed.poll(MAXIMUM_DELAY_SECONDS, TimeUnit.SECONDS);
                }
            }
        });
    }

    public void shutdown() throws InterruptedException {
        this.python.cleanup();
        executor.shutdownNow();
        executor.awaitTermination(0L, TimeUnit.SECONDS);
    }

    public void setTriggerDao(EventTriggerDao triggerDao) {
        this.triggerDao = triggerDao;
    }

    public void setSignalSpoolDao(EventSignalSpoolDao spoolDao) {
        this.signalSpoolDao = spoolDao;
    }

    public void setEventSummaryDao(EventSummaryDao eventSummaryDao) {
        this.eventSummaryDao = eventSummaryDao;
    }

    public void setEventTriggerSubscriptionDao(EventTriggerSubscriptionDao eventTriggerSubscriptionDao) {
        this.eventTriggerSubscriptionDao = eventTriggerSubscriptionDao;
    }

    public void setConnectionManager(AmqpConnectionManager connmgr) {
        this.connectionManager = connmgr;
    }

    protected boolean eventSatisfiesRule(EventSummary evtsummary,
                                         String ruleSource) {
        // set up interpreter environment to evaluate the rule source
        PyDictionary eventdict = new PyDictionary();
        PyDictionary devdict = new PyDictionary();
        PyDictionary componentdict = new PyDictionary();
        PyDictionary servicedict = new PyDictionary();

        // extract event data from most recent occurrence
        if (evtsummary.getOccurrenceCount() > 0) {
            Event event = evtsummary.getOccurrence(0);

            // copy event data to eventdict
            Map<Descriptors.FieldDescriptor, Object> evtdata = event.getAllFields();
            for(Descriptors.FieldDescriptor field : evtdata.keySet()) {
                String fname = field.getName();
                if (fieldPyclassMap.containsKey(fname)) {
                    Object fvalue = evtdata.get(field);
                    eventdict.put(fname, new PyString((String)fvalue));
                }
            }
            eventdict.put("severity", severityAsPyString.get(event.getSeverity()));
            
            if (event.hasActor()) {
                EventActor actor = event.getActor();
                
                if (actor.hasElementTypeId() && actor.hasElementIdentifier()) {
                    final String id = actor.getElementIdentifier();
                    switch (actor.getElementTypeId()) {
                    case DEVICE:
                        devdict.put("name", id);
                        break;
                    case COMPONENT:
                        componentdict.put("name", id);
                        break;
                    case SERVICE:
                        servicedict.put("name", id);
                        break;
                    }
                }
                
                if (actor.hasElementSubTypeId() && actor.hasElementSubIdentifier()) {
                    final String id = actor.getElementSubIdentifier();
                    switch (actor.getElementSubTypeId()) {
                    case DEVICE:
                        devdict.put("name", id);
                        break;
                    case COMPONENT:
                        componentdict.put("name", id);
                        break;
                    case SERVICE:
                        servicedict.put("name", id);
                        break;
                    }
                }    
            }
            
            for (EventDetail detail : event.getDetailsList()) {
                if ("zenoss.device.production_state".equals(detail.getName())) {
                    try {
                        int prodState = Integer.parseInt(detail.getValue(0));
                        devdict.put("production_state", new PyInteger(prodState));
                    } catch (Exception e) {
                        logger.warn("Failed retrieving production state", e);
                    }
                }
                else if ("zenoss.device.priority".equals(detail.getName())) {
                    try {
                        int priority = Integer.parseInt(detail.getValue(0));
                        devdict.put("priority", new PyInteger(priority));
                    } catch (Exception e) {
                        logger.warn("Failed retrieving device priority", e);
                    }
                }
            }
        }

        // add more data from the EventSummary itself
        eventdict.put("count", new PyInteger(evtsummary.getCount()));
        eventdict.put("status", statusAsPyString.get(evtsummary.getStatus()));

        PyObject result;
        try {
            // use rule to build and evaluate a Python lambda expression
            PyFunction fn = ruleFunctionCache.get(ruleSource);
            if (fn == null) {
                fn = (PyFunction)this.python.eval(
                    "lambda evt, dev, component, service : " + ruleSource
                    );
                ruleFunctionCache.put(ruleSource, fn);
            }

            // create vars to pass to rule expression function
            PyObject evtobj = this.toObject.__call__(eventdict);
            PyObject devobj = this.toObject.__call__(devdict);
            PyObject cmpobj = this.toObject.__call__(componentdict);
            PyObject svcobj = this.toObject.__call__(servicedict);

            // evaluate the rule function
            result = fn.__call__(evtobj, devobj, cmpobj, svcobj);
        } catch (PyException pyexc) {
            // evaluating rule raised an exception - treat as "False" eval
            logger.debug("exception raised while evaluating rule: ", pyexc);
            result = new PyInteger(0);
        }

        // return result as a boolean, using Python __nonzero__
        // object-as-bool evaluator
        return result.__nonzero__();
    }

    @Override
    public void processEvent(EventSummary eventSummary)
            throws ZepException {
        Event currentEvent = eventSummary.getOccurrence(0);
        List<EventTrigger> triggers = this.triggerDao.findAll();
        for (EventTrigger trigger : triggers) {
            logger.debug("Checking trigger against event.{}", trigger);

            if (!trigger.getEnabled()) {
                logger.debug("skip trigger {}, not enabled", trigger.getName());
                continue;
            }
            // check to see if this trigger has any subscriptions registered with it
            // if not, continue, if yes, process trigger rule with regard to event.
            List<EventTriggerSubscription> subscriptions = trigger
                    .getSubscriptionsList();
            if (subscriptions.isEmpty()) {
                continue;
            }

            if (trigger.hasRule() && trigger.getRule().hasSource()) {
                Rule rule = trigger.getRule();
                String ruleSource = rule.getSource();

                if (eventSatisfiesRule(eventSummary, ruleSource)) {

                    // handle interval evaluation/buffering
                    for (EventTriggerSubscription subscription : subscriptions) {
                        if (subscription.getDelaySeconds() > 0
                                && currentEvent.getSeverity() != EventSeverity.SEVERITY_CLEAR) {
                            // create spool record for this interval/event
                            EventSignalSpool ess = EventSignalSpool.buildSpool(
                                    subscription, eventSummary);
                            this.signalSpoolDao.create(ess);
                            delayed.put(new SpoolDelayed(subscription
                                    .getDelaySeconds()));
                        } else {
                            this.publishSignal(eventSummary, subscription);
                        }
                    }
                }
            }

            // if this is a CLEAR event, delete spool from the database
            // matching this trigger/event summary pair
            if (currentEvent.getSeverity() == EventSeverity.SEVERITY_CLEAR) {
                // clear out spool for this trigger
                this.signalSpoolDao.delete(trigger.getUuid(), eventSummary.getUuid());
            }
        }
    }

    protected void publishSignal(EventSummary summary,
            EventTriggerSubscription subscription) throws ZepException {
        try {
            Signal.Builder signalBuilder = Signal.newBuilder();
            signalBuilder.setUuid(UUID.randomUUID().toString());
            signalBuilder.setCreatedTime(System.currentTimeMillis());
            signalBuilder.setEvent(summary);
            signalBuilder.setSubscriberUuid(subscription.getSubscriberUuid());
            signalBuilder.setTriggerUuid(subscription.getTriggerUuid());
            signalBuilder
                    .setClear(summary.getStatus() == EventStatus.STATUS_CLEARED);
            if (summary.getOccurrenceCount() > 0) {
                signalBuilder.setMessage(summary.getOccurrence(0).getMessage());
            }
            ExchangeConfiguration destExchange = ZenossQueueConfig.getConfig()
                    .getExchange("$Signals");
            this.connectionManager.publish(destExchange, "zenoss.signal",
                    signalBuilder.build());
        } catch (IOException ioe) {
            throw new ZepException(ioe);
        } catch (AmqpException e) {
            throw new ZepException(e);
        }
    }

    protected int processSpool() throws ZepException {
        logger.debug("Processing signal spool");
        int minRepeatSeconds = -1;
        final long now = System.currentTimeMillis();
        try {
            // get spools that need to be processed
            List<EventSignalSpool> spools = this.signalSpoolDao.findAllDue();
            for (EventSignalSpool spool : spools) {
                int repeatInterval = -1;
                EventSummary eventSummary = this.eventSummaryDao
                        .findByUuid(spool.getEventSummaryUuid());
                EventTriggerSubscription trSub = this.eventTriggerSubscriptionDao
                        .findByUuid(spool.getEventTriggerSubscriptionUuid());
                if (trSub != null && eventSummary != null
                        && eventSummary.getStatus() == EventStatus.STATUS_NEW) {
                    this.publishSignal(eventSummary, trSub);
                    // find subscription for this spool
                    repeatInterval = trSub.getRepeatSeconds();
                    if (repeatInterval > 0) {
                        this.signalSpoolDao.updateFlushTime(
                                        spool.getUuid(),
                                        now + TimeUnit.SECONDS.toMillis(repeatInterval));
                        if (minRepeatSeconds < 0) {
                            minRepeatSeconds = repeatInterval;
                        } else {
                            minRepeatSeconds = Math.min(repeatInterval,
                                    minRepeatSeconds);
                        }
                    }
                }
                if (repeatInterval <= 0) {
                    this.signalSpoolDao.delete(spool.getUuid());
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to process signal spool", e);
        }
        return minRepeatSeconds;
    }

    private static class SpoolDelayed implements Delayed {

        private final int seconds;

        public SpoolDelayed(int seconds) {
            this.seconds = seconds;
        }

        @Override
        public int compareTo(Delayed o) {
            if (o == this) {
                return 0;
            }
            long diff = getDelay(TimeUnit.MILLISECONDS)
                    - o.getDelay(TimeUnit.MILLISECONDS);
            return diff > 0 ? 1 : (diff < 0) ? -1 : 0;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(this.seconds, TimeUnit.SECONDS);
        }
    }
}
