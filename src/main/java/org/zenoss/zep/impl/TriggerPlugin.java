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
import org.zenoss.protobufs.model.Model.ModelElementType;
import org.zenoss.protobufs.zep.Zep.Rule;
import org.zenoss.protobufs.zep.Zep.Signal;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.ZepUtils;
import org.zenoss.zep.dao.EventSignalSpool;
import org.zenoss.zep.dao.EventSignalSpoolDao;
import org.zenoss.zep.dao.EventSummaryDao;
import org.zenoss.zep.dao.EventTriggerDao;
import org.zenoss.zep.dao.EventTriggerSubscriptionDao;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
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

/**
 * Post processing plug-in used to determine if NEW  or CLEARED events match a specified trigger. If the
 * trigger doesn't match, no signal is sent. If the trigger matches and doesn't specify a delay or repeat
 * interval, then it is sent immediately. If the trigger matches and specifies a delay, then a signal is
 * sent only if the event is still in NEW state after the delay. If the trigger specifies a repeat
 * interval, then after the initial signal is sent, the event is checked again after the repeat interval. If
 * the event is still in NEW state after the repeat interval, then another signal is sent and the check is
 * repeated again at the repeat interval.
 *
 * If an event which previously triggered a signal is cleared by another event, a final signal is sent
 * with the clear attribute set to true.
 */
public class TriggerPlugin extends AbstractPostProcessingPlugin {

    private static final Logger logger = LoggerFactory.getLogger(TriggerPlugin.class);

    private EventTriggerDao triggerDao;
    private EventSignalSpoolDao signalSpoolDao;
    private EventSummaryDao eventSummaryDao;
    private EventTriggerSubscriptionDao eventTriggerSubscriptionDao;

    private AmqpConnectionManager connectionManager;

    protected PythonInterpreter python;
    protected PyFunction toObject;
    protected static final Map<EventSeverity, PyString> severityAsPyString = new EnumMap<EventSeverity, PyString>(
            EventSeverity.class);
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

        severityAsPyString.put(EventSeverity.SEVERITY_CLEAR, new PyString("clear"));
        severityAsPyString.put(EventSeverity.SEVERITY_DEBUG, new PyString("debug"));
        severityAsPyString.put(EventSeverity.SEVERITY_INFO, new PyString("info"));
        severityAsPyString.put(EventSeverity.SEVERITY_WARNING, new PyString("warning"));
        severityAsPyString.put(EventSeverity.SEVERITY_ERROR, new PyString("error"));
        severityAsPyString.put(EventSeverity.SEVERITY_CRITICAL, new PyString("critical"));
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

    private void putIdAndUuidInDict(PyDictionary dict, String id, String uuid){
        if (id != null) {
            dict.put("name", id);
        }
        if (uuid != null) {
            dict.put("uuid", uuid);
        }
    }

    protected boolean eventSatisfiesRule(EventSummary evtsummary,
                                         String ruleSource) {
        // set up interpreter environment to evaluate the rule source
        PyDictionary eventdict = new PyDictionary();
        PyDictionary devdict = new PyDictionary();
        PyDictionary elemdict = new PyDictionary();
        PyDictionary subelemdict = new PyDictionary();
        // extract event data from most recent occurrence
        if (evtsummary.getOccurrenceCount() > 0) {
            Event event = evtsummary.getOccurrence(0);

            // copy event data to eventdict
            if (event.hasSummary()) {
                eventdict.put("summary", new PyString(event.getSummary()));
            }
            if (event.hasMessage()) {
                eventdict.put("message", new PyString(event.getMessage()));
            }
            if (event.hasEventClass()) {
                eventdict.put("event_class", new PyString(event.getEventClass()));
            }
            if (event.hasFingerprint()) {
                eventdict.put("fingerprint", new PyString(event.getFingerprint()));
            }
            if (event.hasEventKey()) {
                eventdict.put("event_key", new PyString(event.getEventKey()));
            }
            if (event.hasAgent()) {
                eventdict.put("agent", new PyString(event.getAgent()));
            }
            if (event.hasMonitor()) {
                eventdict.put("monitor", new PyString(event.getMonitor()));
            }
            eventdict.put("severity", severityAsPyString.get(event.getSeverity()));
            eventdict.put("status", new PyString(evtsummary.getStatus().name()));

            if (event.hasActor()) {
                EventActor actor = event.getActor();

                if (actor.hasElementTypeId()) {
                    elemdict.put("type", actor.getElementTypeId().name());
                    final String id = (actor.hasElementIdentifier()) ? actor.getElementIdentifier() : null;
                    final String uuid = actor.hasElementUuid() ? actor.getElementUuid() : null;

                    if (actor.getElementTypeId() == ModelElementType.DEVICE) {
                        this.putIdAndUuidInDict(devdict, id, uuid);
                    }
                    putIdAndUuidInDict(elemdict, id, uuid);
                }

                if (actor.hasElementSubTypeId()) {
                    subelemdict.put("type", actor.getElementSubTypeId().name());
                    final String id = (actor.hasElementSubIdentifier()) ? actor.getElementSubIdentifier() : null;
                    final String uuid = actor.hasElementSubUuid() ? actor.getElementSubUuid() : null;

                    if (actor.getElementSubTypeId() == ModelElementType.DEVICE) {
                        this.putIdAndUuidInDict(devdict, id, uuid);
                    }
                    putIdAndUuidInDict(subelemdict, id, uuid);
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

        PyObject result;
        try {
            // use rule to build and evaluate a Python lambda expression
            PyFunction fn = ruleFunctionCache.get(ruleSource);
            if (fn == null) {
                fn = (PyFunction)this.python.eval(
                    "lambda evt, dev, elem, sub_elem : " + ruleSource
                    );
                ruleFunctionCache.put(ruleSource, fn);
            }

            // create vars to pass to rule expression function
            PyObject evtobj = this.toObject.__call__(eventdict);
            PyObject devobj = this.toObject.__call__(devdict);
            PyObject elemobj = this.toObject.__call__(elemdict);
            PyObject subelemobj = this.toObject.__call__(subelemdict);
            // evaluate the rule function
            result = fn.__call__(evtobj, devobj, elemobj, subelemobj);
        } catch (PyException pyexc) {
            // evaluating rule raised an exception - treat as "False" eval
            logger.warn("exception raised while evaluating rule: " + ruleSource, pyexc);
            result = new PyInteger(0);
        }

        // return result as a boolean, using Python __nonzero__
        // object-as-bool evaluator
        return result.__nonzero__();
    }

    @Override
    public void processEvent(EventSummary eventSummary)
            throws ZepException {
        // Triggers should only be processed for NEW and cleared events
        if (eventSummary.getStatus() == EventStatus.STATUS_NEW ||
            eventSummary.getStatus() == EventStatus.STATUS_CLEARED) {

            List<EventTrigger> triggers = this.triggerDao.findAllEnabled();
            for (EventTrigger trigger : triggers) {
                logger.debug("Checking trigger {} against event {}", trigger, eventSummary);

                // check to see if this trigger has any subscriptions registered with it
                // if not, continue, if yes, process trigger rule with regard to event.
                List<EventTriggerSubscription> subscriptions = trigger.getSubscriptionsList();
                if (subscriptions.isEmpty()) {
                    continue;
                }

                if (trigger.hasRule() && trigger.getRule().hasSource()) {
                    Rule rule = trigger.getRule();
                    String ruleSource = rule.getSource();

                    if (eventSatisfiesRule(eventSummary, ruleSource)) {
                        // handle interval evaluation/buffering
                        for (EventTriggerSubscription subscription : subscriptions) {
                            final int delaySeconds = subscription.getDelaySeconds();
                            final int repeatSeconds = subscription.getRepeatSeconds();

                            // Send signal immediately if no delay
                            if (delaySeconds <= 0) {
                                this.publishSignal(eventSummary, subscription);
                            }

                            // If we have a delay or repeat interval, create a spool record
                            if (delaySeconds > 0 || repeatSeconds > 0) {
                                EventSignalSpool ess = EventSignalSpool.buildSpool(subscription, eventSummary);
                                this.signalSpoolDao.create(ess);

                                int nextDelay = (delaySeconds > 0) ? delaySeconds : repeatSeconds;
                                delayed.put(new SpoolDelayed(nextDelay));
                            }
                        }
                    }
                }
            }
        }
    }

    protected void publishSignal(EventSummary summary, EventTriggerSubscription subscription) throws ZepException {
        try {
            final Event occurrence = summary.getOccurrence(0);
            Signal.Builder signalBuilder = Signal.newBuilder();
            signalBuilder.setUuid(UUID.randomUUID().toString());
            signalBuilder.setCreatedTime(System.currentTimeMillis());
            signalBuilder.setEvent(summary);
            signalBuilder.setSubscriberUuid(subscription.getSubscriberUuid());
            signalBuilder.setTriggerUuid(subscription.getTriggerUuid());
            signalBuilder.setClear(summary.getStatus() == EventStatus.STATUS_CLEARED);
            signalBuilder.setMessage(occurrence.getMessage());
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
        int minRepeatSeconds = Integer.MAX_VALUE;
        final long now = System.currentTimeMillis();
        try {
            // get spools that need to be processed
            List<EventSignalSpool> spools = this.signalSpoolDao.findAllDue();
            for (EventSignalSpool spool : spools) {
                EventSummary eventSummary = this.eventSummaryDao.findByUuid(spool.getEventSummaryUuid());
                EventStatus status = (eventSummary != null) ? eventSummary.getStatus() : null;

                EventTriggerSubscription trSub = this.eventTriggerSubscriptionDao
                        .findByUuid(spool.getSubscriptionUuid());

                // Ensure trigger is still enabled
                // TODO: Should we re-evaluate event against rule again?
                boolean triggerEnabled = false;
                if (trSub != null) {
                    EventTrigger trigger = this.triggerDao.findByUuid(trSub.getTriggerUuid());
                    if (trigger != null) {
                        triggerEnabled = trigger.getEnabled();
                    }
                }

                int repeatInterval = -1;
                if (triggerEnabled && (status == EventStatus.STATUS_NEW || status == EventStatus.STATUS_CLEARED)) {
                    this.publishSignal(eventSummary, trSub);

                    // If the event is still in NEW status, repeat the signal
                    if (status == EventStatus.STATUS_NEW) {
                        repeatInterval = trSub.getRepeatSeconds();
                    }
                }

                // If we don't repeat the signal, delete it from the spool
                if (repeatInterval <= 0) {
                    this.signalSpoolDao.delete(spool.getUuid());
                }
                else {
                    this.signalSpoolDao.updateFlushTime(spool.getUuid(),
                            now + TimeUnit.SECONDS.toMillis(repeatInterval));
                    minRepeatSeconds = Math.min(repeatInterval, minRepeatSeconds);
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
