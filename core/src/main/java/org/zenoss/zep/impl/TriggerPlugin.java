/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2012, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.impl;

import com.google.common.base.Splitter;
import org.python.core.Py;
import org.python.core.PyDictionary;
import org.python.core.PyException;
import org.python.core.PyFunction;
import org.python.core.PyInteger;
import org.python.core.PyList;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.core.PySyntaxError;
import org.python.util.PythonInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;
import org.zenoss.amqp.AmqpConnectionManager;
import org.zenoss.amqp.AmqpException;
import org.zenoss.amqp.ExchangeConfiguration;
import org.zenoss.amqp.ZenossQueueConfig;
import org.zenoss.protobufs.model.Model.ModelElementType;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.protobufs.zep.Zep.EventDetail;
import org.zenoss.protobufs.zep.Zep.EventStatus;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventTrigger;
import org.zenoss.protobufs.zep.Zep.EventTriggerSubscription;
import org.zenoss.protobufs.zep.Zep.Signal;
import org.zenoss.zep.UUIDGenerator;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.ZepUtils;
import org.zenoss.zep.dao.EventSignalSpool;
import org.zenoss.zep.dao.EventSignalSpoolDao;
import org.zenoss.zep.dao.EventStoreDao;
import org.zenoss.zep.dao.EventSummaryDao;
import org.zenoss.zep.dao.EventTriggerDao;
import org.zenoss.zep.dao.EventTriggerSubscriptionDao;
import org.zenoss.zep.plugins.EventPostIndexContext;
import org.zenoss.zep.plugins.EventPostIndexPlugin;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.zenoss.zep.ZepConstants.*;

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
public class TriggerPlugin extends EventPostIndexPlugin {

    private static final Logger logger = LoggerFactory.getLogger(TriggerPlugin.class);

    private EventTriggerDao triggerDao;
    private EventSignalSpoolDao signalSpoolDao;
    private EventStoreDao eventStoreDao;
    private EventSummaryDao eventSummaryDao;
    private EventTriggerSubscriptionDao eventTriggerSubscriptionDao;
    private UUIDGenerator uuidGenerator;

    private AmqpConnectionManager connectionManager;
    private ExchangeConfiguration destinationExchange;

    /**
     * Caches the result of compiling a trigger rule. Contains the original rule source, and the compiled PyFunction
     * from the source. The PyFunction can be null if the rule source is invalid and can't be compiled to valid
     * Python. It is cached no matter what to prevent trying to compile an invalid rule over and over again.
     */
    static final class TriggerRuleCache {
        private final String ruleSource;
        private final PyFunction pyFunction;
        
        public TriggerRuleCache(String ruleSource, PyFunction pyFunction) {
            this.ruleSource = ruleSource;
            this.pyFunction = pyFunction;
        }

        public String getRuleSource() {
            return ruleSource;
        }

        public PyFunction getPyFunction() {
            return pyFunction;
        }

        @Override
        public String toString() {
            return "TriggerRuleCache{" +
                    "ruleSource='" + ruleSource + '\'' +
                    '}';
        }
    }

    // Default size of trigger rule cache - can be overridden by specifying plugin.TriggerPlugin.triggerRuleCacheSize
    // in the zeneventserver.conf file.
    private static final int DEFAULT_TRIGGER_RULE_CACHE_SIZE = 200;

    // Map of Trigger UUID -> TriggerRuleCache.
    protected Map<String, TriggerRuleCache> triggerRuleCache;

    // The maximum amount of time to wait between processing the signal spool.
    private static final long MAXIMUM_DELAY_MS = TimeUnit.SECONDS.toMillis(60);

    private TaskScheduler scheduler;
    private ScheduledFuture<?> spoolFuture;
    PythonHelper pythonHelper = new PythonHelper();

    /**
     * Helper class to enable lazy-initialization of Jython. Initializing the runtime and compiling code is expensive
     * to perform on each startup - better to do it when we first need it for evaluating triggers.
     */
    static final class PythonHelper {
        private volatile boolean initialized = false;

        private PythonInterpreter python;
        private PyFunction toObject;

        private synchronized void initialize() {
            if (initialized) {
                return;
            }
            logger.info("Initializing Jython");
            this.initialized = true;
            PythonInterpreter.initialize(System.getProperties(), new Properties(), new String[0]);

            this.python = new PythonInterpreter();

            // define basic infrastructure class for evaluating rules
            this.python.exec(
                "class DictAsObj(object):\n" +
                "    def __init__(self, **kwargs):\n" +
                "        def setobj(x, y):\n" +
                "            o = getattr(x, y) if hasattr(x, y) else DictAsObj()\n" +
                "            setattr(x, y, o)\n" +
                "            return o\n" +
                "        for k,v in kwargs.iteritems():\n" +
                "            segments = k.split('.')\n" +
                "            leaf = reduce(\n" +
                "                lambda x, y: setobj(x, y),\n" +
                "                segments[:-1],\n" +
                "                self\n" +
                "            )\n" +
                "            setattr(leaf, segments[-1], v)");

            // expose to Java a Python dict->DictAsObj conversion function
            this.toObject = (PyFunction)this.python.eval("lambda dd : DictAsObj(**dd)");
            logger.info("Completed Jython initialization");
        }

        public PythonInterpreter getPythonInterpreter() {
            if (!initialized) {
                initialize();
            }
            return this.python;
        }

        public PyFunction getToObject() {
            if (!initialized) {
                initialize();
            }
            return this.toObject;
        }

        public void cleanup() {
            if (initialized) {
                this.python.cleanup();
            }
        }
    }


    public TriggerPlugin() throws IOException {
        destinationExchange = ZenossQueueConfig.getConfig().getExchange("$Signals");
    }

    @Autowired
    public void setTaskScheduler(TaskScheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Autowired
    public void setUuidGenerator(UUIDGenerator uuidGenerator) {
        this.uuidGenerator = uuidGenerator;
    }

    @Override
    public void start(Map<String, String> properties) {
        int triggerRuleCacheSize = this.getTriggerRuleCacheSize();
        logger.info("TriggerPlugin trigger rule cache size: {}", triggerRuleCacheSize);
        Map<String,TriggerRuleCache> boundedMap = ZepUtils.createBoundedMap(triggerRuleCacheSize);
        this.triggerRuleCache = Collections.synchronizedMap(boundedMap);
        super.start(properties);
        scheduleSpool();
    }

    @Override
    public void stop() {
        this.pythonHelper.cleanup();
        if (spoolFuture != null) {
            spoolFuture.cancel(true);
        }
    }

    private int getTriggerRuleCacheSize() {
        int triggerRuleCacheSize = DEFAULT_TRIGGER_RULE_CACHE_SIZE;
        String cacheSize = properties.get("triggerRuleCacheSize");
        if (cacheSize != null) {
            try {
                triggerRuleCacheSize = Integer.parseInt(cacheSize.trim());
            } catch (NumberFormatException e) {
                logger.warn("Invalid trigger rule cache size: {}", cacheSize);
            }
        }
        return triggerRuleCacheSize;
    }

    private boolean cacheIsFull(Map<String, TriggerRuleCache> cache) {
        return cache.size() >= this.getTriggerRuleCacheSize();
    }

    private void scheduleSpool() {
        if (spoolFuture != null) {
            spoolFuture.cancel(false);
        }
        Trigger trigger = new Trigger() {
            @Override
            public Date nextExecutionTime(TriggerContext triggerContext) {
                Date nextExecution = null;
                try {
                    long nextFlushTime = signalSpoolDao.getNextFlushTime();
                    if (nextFlushTime > 0) {
                        nextExecution = new Date(nextFlushTime);
                        logger.debug("Next flush time: {}", nextExecution);
                    }
                } catch (Exception e) {
                    logger.warn("Exception getting next flush time", e);
                }
                if (nextExecution == null) {
                    nextExecution = new Date(System.currentTimeMillis() + MAXIMUM_DELAY_MS);
                }
                return nextExecution;
            }
        };
        Runnable runnable = new ThreadRenamingRunnable(new Runnable() {
            @Override
            public void run() {
                processSpool(System.currentTimeMillis());
            }
        }, "ZEP_TRIGGER_PLUGIN_SPOOL");
        spoolFuture = scheduler.schedule(runnable, trigger);
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

    public void setEventStoreDao(EventStoreDao eventStoreDao) {
        this.eventStoreDao = eventStoreDao;
    }

    /**
     * Local context class used to store the Python objects created from the event which are passed in to the
     * trigger's rule for evaluation.
     */
    static class RuleContext {
        PyObject event;
        PyObject device;
        PyObject element;
        PyObject subElement;

        private RuleContext() {
        }

        private static void putTitleAndUuidInDict(PyDictionary dict, String title, String uuid) {
            if (title != null) {
                dict.put("name", title);
            }
            if (uuid != null) {
                dict.put("uuid", uuid);
            }
        }

        private static final Splitter ORGANIZER_SPLITTER = Splitter.on('/').omitEmptyStrings();

        /**
         * Given a list of organizers, returns a list containing those same organizers plus
         * any parent organizers. For example, ['/First/Second/Third','/OtherFirst/OtherSecond']
         * will return ['/First', '/First/Second', '/First/Second/Third', '/OtherFirst',
         * '/OtherFirst/OtherSecond'].
         *
         * @param baseOrganizers List of most-specific organizer names.
         * @return A list containing all of the organizers plus their parent organizers.
         */
        private static List<String> includeParentOrganizers(List<String> baseOrganizers) {
            Set<String> allOrganizers = new TreeSet<String>();
            for (String organizer : baseOrganizers) {
                final StringBuilder sb = new StringBuilder(organizer.length());
                for (String subOrganizer : ORGANIZER_SPLITTER.split(organizer)) {
                    sb.append('/').append(subOrganizer);
                    allOrganizers.add(sb.toString());
                }
            }
            return new ArrayList<String>(allOrganizers);
        }

        /**
         * Creates a rule context from the toObject function and event summary.
         *
         * @param toObject The toObject function which converts a dictionary to the appropriate object.
         * @param evtsummary The event to convert to a context.
         * @return A rule context for the event.
         */
        public static RuleContext createContext(PyFunction toObject, EventSummary evtsummary) {
            // set up interpreter environment to evaluate the rule source
            PyDictionary eventdict = new PyDictionary();
            PyDictionary devdict = new PyDictionary();
            PyDictionary elemdict = new PyDictionary();
            PyDictionary subelemdict = new PyDictionary();

            // Match old behavior (pre-4.x)
            int prodState = 0;
            int devicePriority = DEVICE_PRIORITY_NORMAL;
            String ipAddress = "";
            List<String> systemsAndParents = Collections.emptyList();
            List<String> groupsAndParents = Collections.emptyList();
            String location = "";
            String deviceClass = "";

            // extract event data from most recent occurrence
            Event event = evtsummary.getOccurrence(0);

            // copy event data to eventdict
            eventdict.put("summary", new PyString(event.getSummary()));
            eventdict.put("message", new PyString(event.getMessage()));
            eventdict.put("event_class", new PyString(event.getEventClass()));
            eventdict.put("fingerprint", new PyString(event.getFingerprint()));
            eventdict.put("event_key", new PyString(event.getEventKey()));
            eventdict.put("agent", new PyString(event.getAgent()));
            eventdict.put("monitor", new PyString(event.getMonitor()));
            eventdict.put("severity", event.getSeverity().getNumber());
            eventdict.put("event_class_key", new PyString(event.getEventClassKey()));
            if (event.hasSyslogPriority()) {
                eventdict.put("syslog_priority", new PyInteger(event.getSyslogPriority().getNumber()));
            }
            if (event.hasSyslogFacility()) {
                eventdict.put("syslog_facility", event.getSyslogFacility());
            }
            if (event.hasNtEventCode()) {
                eventdict.put("nt_event_code", event.getNtEventCode());
            }
            // Initialize to empty attributes on elem and subelem in case a rule references
            // it and they do not exist
            putTitleAndUuidInDict(subelemdict, "", "");
            putTitleAndUuidInDict(elemdict, "", "");

            EventActor actor = event.getActor();

            if (actor.hasElementTypeId()) {
                if (actor.getElementTypeId() == ModelElementType.DEVICE) {
                    devdict = elemdict;
                }

                elemdict.put("type", actor.getElementTypeId().name());
                final String id = (actor.hasElementIdentifier()) ? actor.getElementIdentifier() : null;
                final String title = (actor.hasElementTitle()) ? actor.getElementTitle() : id;
                final String uuid = actor.hasElementUuid() ? actor.getElementUuid() : null;

                putTitleAndUuidInDict(elemdict, title, uuid);
            }

            if (actor.hasElementSubTypeId()) {
                if (actor.getElementSubTypeId() == ModelElementType.DEVICE) {
                    devdict = subelemdict;
                }

                subelemdict.put("type", actor.getElementSubTypeId().name());
                final String id = (actor.hasElementSubIdentifier()) ? actor.getElementSubIdentifier() : null;
                final String title = (actor.hasElementSubTitle()) ? actor.getElementSubTitle() : id;
                final String uuid = actor.hasElementSubUuid() ? actor.getElementSubUuid() : null;

                putTitleAndUuidInDict(subelemdict, title, uuid);
            }

            for (EventDetail detail : event.getDetailsList()) {
                final String detailName = detail.getName();
                // This should never happen
                if (detail.getValueCount() == 0) {
                    continue;
                }
                final String singleDetailValue = detail.getValue(0);
                
                if (DETAIL_DEVICE_PRODUCTION_STATE.equals(detailName)) {
                    try {
                        prodState = Integer.parseInt(singleDetailValue);
                    } catch (NumberFormatException e) {
                        logger.warn("Failed retrieving production state", e);
                    }
                }
                else if (DETAIL_DEVICE_PRIORITY.equals(detailName)) {
                    try {
                        devicePriority = Integer.parseInt(singleDetailValue);
                    } catch (NumberFormatException e) {
                        logger.warn("Failed retrieving device priority", e);
                    }
                }
                else if (DETAIL_DEVICE_CLASS.equals(detailName)) {
                    // expect that this is a single-value detail.
                    deviceClass = singleDetailValue;
                }
                else if (DETAIL_DEVICE_SYSTEMS.equals(detailName)) {
                    // expect that this is a multi-value detail.
                    systemsAndParents = includeParentOrganizers(detail.getValueList());
                }
                else if (DETAIL_DEVICE_GROUPS.equals(detailName)) {
                    // expect that this is a multi-value detail.
                    groupsAndParents = includeParentOrganizers(detail.getValueList());
                }
                else if (DETAIL_DEVICE_IP_ADDRESS.equals(detailName)) {
                    // expect that this is a single-value detail.
                    ipAddress = singleDetailValue;
                }
                else if (DETAIL_DEVICE_LOCATION.equals(detailName)) {
                    // expect that this is a single-value detail.
                    location = singleDetailValue;
                }
                else {
                    eventdict.put(detailName, new PyString(singleDetailValue));
                }
            }

            devdict.put("device_class", new PyString(deviceClass));
            devdict.put("production_state", new PyInteger(prodState));
            devdict.put("priority", new PyInteger(devicePriority));
            devdict.put("groups", new PyList(groupsAndParents));
            devdict.put("systems", new PyList(systemsAndParents));
            devdict.put("ip_address", new PyString(ipAddress));
            devdict.put("location", new PyString(location));

            // add more data from the EventSummary itself
            eventdict.put("status", evtsummary.getStatus().getNumber());
            eventdict.put("count", new PyInteger(evtsummary.getCount()));
            eventdict.put("current_user_name", new PyString(evtsummary.getCurrentUserName()));

            RuleContext ctx = new RuleContext();
            // create vars to pass to rule expression function
            ctx.event = toObject.__call__(eventdict);
            ctx.device = toObject.__call__(devdict);
            ctx.element = toObject.__call__(elemdict);
            ctx.subElement = toObject.__call__(subelemdict);
            return ctx;
        }
    }

    int cacheSizeWarningCounter = 0;

    protected boolean eventSatisfiesRule(RuleContext ruleContext, String triggerUuid, String ruleSource) {
        PyObject result;
        try {
            // check to see if the cache is full and log an error if so
            if (this.cacheIsFull(this.triggerRuleCache)) {
                ++cacheSizeWarningCounter;
                if (cacheSizeWarningCounter % 100 == 0) {
                    logger.error("Trigger rule cache is full ({}); consider reconfiguring zeneventserver, making it larger",
                            this.getTriggerRuleCacheSize());
                    cacheSizeWarningCounter = 0;
                }
            }
            // use rule to build and evaluate a Python lambda expression
            TriggerRuleCache cacheItem = triggerRuleCache.get(triggerUuid);
            PyFunction fn = null;
            if (cacheItem == null || !cacheItem.getRuleSource().equals(ruleSource)) {
                try {
                    fn = (PyFunction)this.pythonHelper.getPythonInterpreter().eval(
                            "lambda evt, dev, elem, sub_elem : " + ruleSource
                    );
                } catch (PySyntaxError e) {
                    String fmt = Py.formatException(e.type, e.value);
                    logger.warn("syntax error exception raised while compiling rule: {}, {}", ruleSource, fmt);
                }
                // Cache result of trigger evaluation (even if it failed to compile). This will prevent trying to
                // recompile the same invalid rule over and over again.
                triggerRuleCache.put(triggerUuid, new TriggerRuleCache(ruleSource, fn));
            }
            else {
                fn = cacheItem.getPyFunction();
            }
            if (fn == null) {
                logger.debug("Invalid rule source: {}", ruleSource);
                return false;
            }
            // evaluate the rule function
            result = fn.__call__(ruleContext.event, ruleContext.device, ruleContext.element, ruleContext.subElement);
        } catch (PySyntaxError pysynerr) {
            // evaluating rule raised an exception - treat as "False" eval
            String fmt = Py.formatException(pysynerr.type, pysynerr.value);
            logger.warn("syntax error exception raised while compiling rule: {}, {}", ruleSource, fmt);
            result = new PyInteger(0);
        } catch (PyException pyexc) {
            // evaluating rule raised an exception - treat as "False" eval
            // If it's an AttributeError it just means the event doesn't have a value for the field
            // and an eval of False is fine. Otherwise we should log in case there's a real issue.
            if (!pyexc.match(Py.AttributeError)) {
                String fmt = Py.formatException(pyexc.type, pyexc.value);
                logger.warn("exception raised while evaluating rule: {}, {}", ruleSource, fmt);
            }
            else if (logger.isDebugEnabled()) {
                String fmt = Py.formatException(pyexc.type, pyexc.value);
                logger.debug("AttributeError raised while evaluating rule: {}, {}", ruleSource, fmt);
            }
            result = new PyInteger(0);
        }

        // return result as a boolean, using Python __nonzero__
        // object-as-bool evaluator
        return result.__nonzero__();
    }

    private static class BatchIndexState {
        private List<EventTrigger> triggers;
        private Map<String, List<EventTriggerSubscription>> triggerSubscriptions =
                new HashMap<String, List<EventTriggerSubscription>>();
        private Map<String, EventSummary> eventsToDeleteFromSpool = new HashMap<String, EventSummary>();
    }

    private final ThreadLocal<BatchIndexState> batchState = new ThreadLocal<BatchIndexState>();

    @Override
    public void startBatch(EventPostIndexContext context) throws Exception {
        // Ignore events in the archive.
        if (context.isArchive()) {
            return;
        }
        batchState.set(new BatchIndexState());
    }

    @Override
    public void endBatch(EventPostIndexContext context) throws Exception {
        // Ignore events in the archive.
        if (context.isArchive()) {
            return;
        }
        BatchIndexState state = batchState.get();
        if (!state.eventsToDeleteFromSpool.isEmpty()) {
            Set<String> eventUuids = state.eventsToDeleteFromSpool.keySet();
            List<EventSignalSpool> spools = signalSpoolDao.findAllByEventSummaryUuids(eventUuids);
            for (EventSignalSpool spool : spools) {
                if (spool.isSentSignal()) {
                    logger.debug("sending clear signal for event: {}", spool.getEventSummaryUuid());
                    EventTriggerSubscription subscription =
                            eventTriggerSubscriptionDao.findByUuid(spool.getSubscriptionUuid());
                    EventSummary eventSummary = state.eventsToDeleteFromSpool.get(spool.getEventSummaryUuid());
                    publishSignal(eventSummary, subscription);
                } else {
                    logger.debug("Skipping sending of clear signal for event {} and subscription {} - !sentSignal",
                            spool.getEventSummaryUuid(), spool.getSubscriptionUuid());
                }
            }
            signalSpoolDao.deleteByEventSummaryUuids(eventUuids);
        }
        batchState.remove();
    }

    @Override
    public void preProcessEvents(Collection<EventSummary> eventSummaries, EventPostIndexContext context) throws ZepException {
        Set<String> uuids = new HashSet<String>(eventSummaries.size());
        for (EventSummary event : eventSummaries)
            uuids.add(event.getUuid());
        List<EventSignalSpool> spools = this.signalSpoolDao.findAllByEventSummaryUuids(uuids);
        for (EventSignalSpool spool : spools) {
            rememberSpool(spool, context);
        }
    }

    private void rememberSpool(EventSignalSpool spool, EventPostIndexContext context) {
        Map<String, Map<String, EventSignalSpool>> state = getStateFromContext(context);
        Map<String, EventSignalSpool> m = state.get(spool.getSubscriptionUuid());
        if (m == null) {
            m = new HashMap<String, EventSignalSpool>();
            state.put(spool.getSubscriptionUuid(), m);
        }
        m.put(spool.getEventSummaryUuid(), spool);
    }

    private EventSignalSpool getSpoolBySubscriptionAndEventSummary(EventPostIndexContext context,
                                                                   EventTriggerSubscription subscription,
                                                                   EventSummary eventSummary) {
        Map<String, Map<String, EventSignalSpool>> state = getStateFromContext(context);
        Map<String, EventSignalSpool> m = state.get(subscription.getUuid());
        if (m == null) return null;
        return m.get(eventSummary.getUuid());
    }

    private Map<String,Map<String,EventSignalSpool>> getStateFromContext(EventPostIndexContext context) {
        Map<String,Map<String,EventSignalSpool>> state = (Map<String, Map<String, EventSignalSpool>>) context.getPluginState(this);
        if (state == null) {
            state = new HashMap<String, Map<String, EventSignalSpool>>();
            context.setPluginState(this, state);
        }
        return state;
    }

    @Override
    public void processEvent(EventSummary eventSummary, EventPostIndexContext context) throws ZepException {
        // Ignore events in the archive.
        if (context.isArchive()) {
            return;
        }
        final EventStatus evtstatus = eventSummary.getStatus();

        if (OPEN_STATUSES.contains(evtstatus)) {
            processOpenEvent(eventSummary, context);
        } else {
            batchState.get().eventsToDeleteFromSpool.put(eventSummary.getUuid(), eventSummary);
        }
    }

    private void processOpenEvent(EventSummary eventSummary, EventPostIndexContext context) throws ZepException {
        final long now = System.currentTimeMillis();
        BatchIndexState state = batchState.get();
        List<EventTrigger> triggers = state.triggers;
        if (triggers == null) {
            triggers = this.triggerDao.findAllEnabled();
            state.triggers = triggers;
        }

        // iterate over all enabled triggers to see if any rules will match
        // for this event summary
        boolean rescheduleSpool = false;

        RuleContext ruleContext = null;

        if (!triggers.isEmpty()) {
            logger.debug("Event: {}", eventSummary);
        }

        for (EventTrigger trigger : triggers) {

            // verify trigger has a defined rule
            if (!(trigger.hasRule() && trigger.getRule().hasSource())) {
                continue;
            }
            // confirm trigger has any subscriptions registered with it
            List<EventTriggerSubscription> subscriptions = state.triggerSubscriptions.get(trigger.getUuid());
            if (subscriptions == null) {
                subscriptions = trigger.getSubscriptionsList();
                state.triggerSubscriptions.put(trigger.getUuid(), subscriptions);
            }
            if (subscriptions.isEmpty()) {
                continue;
            }

            final String ruleSource = trigger.getRule().getSource();

            // Determine if event matches trigger rule
            if (ruleContext == null) {
                ruleContext = RuleContext.createContext(this.pythonHelper.getToObject(), eventSummary);
            }
            final boolean eventSatisfiesRule = eventSatisfiesRule(ruleContext, trigger.getUuid(), ruleSource);

            if (eventSatisfiesRule) {
                logger.debug("Trigger {} ({}) MATCHES", trigger.getName(), ruleSource);
            }
            else {
                logger.debug("Trigger {} ({}) DOES NOT MATCH", trigger.getName(), ruleSource);
            }

            // handle interval evaluation/buffering
            for (EventTriggerSubscription subscription : subscriptions) {
                final int delaySeconds = subscription.getDelaySeconds();
                final int repeatSeconds = subscription.getRepeatSeconds();

                EventSignalSpool currentSpool = getSpoolBySubscriptionAndEventSummary(context, subscription,eventSummary);
                boolean spoolExists = (currentSpool != null);
                boolean spoolModified = false;

                if (eventSatisfiesRule) {
                    logger.debug("subscriber: {}, delay: {}, repeat: {}, existing spool: {}",
                            new Object[] { subscription.getSubscriberUuid(), delaySeconds, repeatSeconds, spoolExists });
                }

                boolean onlySendInitial = subscription.getSendInitialOccurrence();

                // If the rule wasn't satisfied
                if (!eventSatisfiesRule) {
                    // If the rule previously matched and now no longer matches, ensure that the
                    // repeated signaling will not occur again.
                    if (spoolExists && currentSpool.getFlushTime() < Long.MAX_VALUE) {
                        logger.debug("Event previously matched trigger - disabling repeats");
                        currentSpool.setFlushTime(Long.MAX_VALUE);
                        spoolModified = true;
                        rescheduleSpool = true;
                    }
                }
                // Send signal immediately if no delay
                else if (delaySeconds <= 0) {
                    if (!onlySendInitial) {
                        logger.debug("delay <= 0 and !onlySendInitial, send signal");
                        this.publishSignal(eventSummary, subscription);
                        
                        if (!spoolExists) {
                            currentSpool = EventSignalSpool.buildSpool(subscription, eventSummary, this.uuidGenerator);
                            currentSpool.setSentSignal(true);
                            this.signalSpoolDao.create(currentSpool);
                            rescheduleSpool = true;
                        }
                        else if (!currentSpool.isSentSignal()) {
                            currentSpool.setSentSignal(true);
                            spoolModified = true;
                        }
                    }
                    else {
                        if (!spoolExists) {
                            logger.debug("delay <=0 and spool doesn't exist, send signal");
                            this.publishSignal(eventSummary, subscription);
                            
                            currentSpool = EventSignalSpool.buildSpool(subscription, eventSummary, this.uuidGenerator);
                            currentSpool.setSentSignal(true);
                            this.signalSpoolDao.create(currentSpool);
                            rescheduleSpool = true;
                        }
                        else {
                            if (repeatSeconds > 0 &&
                                currentSpool.getFlushTime() > now + TimeUnit.SECONDS.toMillis(repeatSeconds)) {
                                logger.debug("adjust spool flush time to reflect new repeat seconds");
                                currentSpool.setFlushTime(now + TimeUnit.SECONDS.toMillis(repeatSeconds));
                                spoolModified = true;
                                rescheduleSpool = true;
                            }
                        }
                    }
                }
                else {
                    // delaySeconds > 0
                    if (!spoolExists) {
                        currentSpool = EventSignalSpool.buildSpool(subscription, eventSummary, this.uuidGenerator);
                        this.signalSpoolDao.create(currentSpool);
                        rescheduleSpool = true;
                    }
                    else {
                        if (repeatSeconds == 0) {
                            if (!onlySendInitial && currentSpool.getFlushTime() == Long.MAX_VALUE) {
                                currentSpool.setFlushTime(now + TimeUnit.SECONDS.toMillis(delaySeconds));
                                spoolModified = true;
                                rescheduleSpool = true;
                            }
                        }
                        else {
                            if (currentSpool.getFlushTime() > now + TimeUnit.SECONDS.toMillis(repeatSeconds)) {
                                currentSpool.setFlushTime(now + TimeUnit.SECONDS.toMillis(repeatSeconds));
                                spoolModified = true;
                                rescheduleSpool = true;
                            }
                        }
                    }
                }
                
                if (spoolModified) {
                    this.signalSpoolDao.update(currentSpool);
                }
            }
        }
        if (rescheduleSpool) {
            scheduleSpool();
        }
    }

    protected void publishSignal(EventSummary eventSummary, EventTriggerSubscription subscription) throws ZepException {
        Event occurrence = eventSummary.getOccurrence(0);
        Signal.Builder signalBuilder = Signal.newBuilder();
        signalBuilder.setUuid(uuidGenerator.generate().toString());
        signalBuilder.setCreatedTime(System.currentTimeMillis());
        signalBuilder.setEvent(eventSummary);
        signalBuilder.setSubscriberUuid(subscription.getSubscriberUuid());
        signalBuilder.setTriggerUuid(subscription.getTriggerUuid());
        signalBuilder.setMessage(occurrence.getMessage());
        signalBuilder.setClear(CLOSED_STATUSES.contains(eventSummary.getStatus()));
        if (EventStatus.STATUS_CLEARED == eventSummary.getStatus()) {
            // Look up event which cleared this one
            EventSummary clearEventSummary = this.eventStoreDao.findByUuid(eventSummary.getClearedByEventUuid());
            if (clearEventSummary != null) {
                signalBuilder.setClearEvent(clearEventSummary);
            } else {
                logger.warn("Unable to look up clear event with UUID: {}", eventSummary.getClearedByEventUuid());
            }
        }
        Signal signal = signalBuilder.build();
        logger.debug("Publishing signal: {}", signal);
        try {
            this.connectionManager.publish(destinationExchange, "zenoss.signal", signal);
        } catch (AmqpException e) {
            throw new ZepException(e);
        }
    }

    protected synchronized void processSpool(long processCutoffTime) {
        logger.debug("Processing signal spool");
        try {
            // TODO: This should be refactored to have findAllDue return consistent set of spool, event, subscription,
            // and trigger.

            // get spools that need to be processed
            List<EventSignalSpool> spools = this.signalSpoolDao.findAllDue();
            List<String> spoolsToDelete = new ArrayList<String>(spools.size());

            for (EventSignalSpool spool : spools) {
                EventSummary eventSummary = this.eventSummaryDao.findByUuid(spool.getEventSummaryUuid());
                EventStatus status = (eventSummary != null) ? eventSummary.getStatus() : null;

                // These should have been deleted when the event was run through the TriggerPlugin when the status
                // changed, but just in case delete them as the event is now in a closed state.
                if (!OPEN_STATUSES.contains(status)) {
                    spoolsToDelete.add(spool.getUuid());
                    continue;
                }

                EventTriggerSubscription trSub = this.eventTriggerSubscriptionDao
                        .findByUuid(spool.getSubscriptionUuid());
                if (trSub == null) {
                    logger.debug("Current spool entry no longer valid (subscription deleted), skipping: {}",
                            spool.getUuid());
                    continue;
                }

                // Check to see if trigger is still enabled
                EventTrigger trigger = this.triggerDao.findByUuid(trSub.getTriggerUuid());
                if (trigger == null) {
                    logger.debug("Current spool entry no longer valid (trigger deleted), skipping: {}",
                            spool.getUuid());
                    continue;
                }

                if (trigger.getEnabled()) {
                    publishSignal(eventSummary, trSub);
                    if (!spool.isSentSignal()) {
                        spool.setSentSignal(true);
                    }
                }

                int repeatInterval = trSub.getRepeatSeconds();

                // Schedule the next repeat
                if (repeatInterval > 0) {
                    long nextFlush = processCutoffTime + TimeUnit.SECONDS.toMillis(repeatInterval);
                    spool.setFlushTime(nextFlush);
                }
                else {
                    // Update the existing spool entry to make sure it won't send again
                    spool.setFlushTime(Long.MAX_VALUE);
                }
                this.signalSpoolDao.update(spool);
            }
            if (!spoolsToDelete.isEmpty()) {
                this.signalSpoolDao.delete(spoolsToDelete);
            }

        } catch (Exception e) {
            logger.warn("Failed to process signal spool", e);
        }
    }
}
