/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2013, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.zenoss.protobufs.zep.Zep;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.zep.*;
import org.zenoss.zep.dao.FlapTrackerDao;
import org.zenoss.zep.dao.impl.EventDaoUtils;
import org.zenoss.zep.dao.ConfigDao;
import org.zenoss.zep.events.ZepConfigUpdatedEvent;
import org.zenoss.zep.events.ZepEvent;
import org.zenoss.zep.plugins.EventPreCreateContext;
import org.zenoss.zep.plugins.EventPreCreatePlugin;
import java.util.Map;

/**
 * Summary:
 * This plugin runs before each event is processed and detects if the
 * device emitting the event is currently in a state of flapping.
 *
 * Glossary:
 * 1. Detection - a set of events, as identified by the clear id, is
 * opened and cleared several times in a given time interval.
 * 2. Notification suppression - only alert once when an event flaps several times,
 * this is outside the scope of this project
 * 3. Flap - when a given event set as identified by the clearid goes from a
 * less than error severity to an error or greater severity.
 * 4. Clear Id - This is the clear fingerprint hash, it is how I tie a set of events
 * together.
 * 
 * Implementation:
 * Each time an event is indexed we will add it
 * to a FlapTracker that has the following properties:
 * 
 * key: event clear id
 * value: Object - last status for the clear id
 * - list of timestamps for state transitions (or flaps)
 * 
 * By the list of timestamps for state transitions we mean that every
 * time we get an event with a matching clearid and a severity greater
 * than SeverityThreshold and the previous severity was less than SeverityThreshold the
 * timestamp will be added to that entry. In other words, depending on the
 * severity of the event the event set will either be in a "good state"
 * (e.g. below warning) or a "bad state" (e.g. warning or above). We need to keep
 * track of each time the event set goes from "bad" to "good".
 * 
 * If the length of the list of timestamps exceeds a configurable length in
 * a given interval we can defined the events for that clear id to be
 * flapping. Any timestamps that fall outside of the given interval will
 * be discarded.
 * When we detect that a given clear id is flapping, we will send out a
 * separate event to denote that that device and component is in a state
 * of flapping.  There will not be a separate "unflapping" event. When an
 * flapping event is sent we will clear the list of transitions. This is
 * so we do not repeatedly send flapping events.
 * 
 * Example:
 * To illustrate how this works here is an example series of events
 * 
 * 
 * This is assuming we need 3 "bad" to "good" state transitions to qualify as flapping. Our time interval
 * is 8 seconds.
 * 
 * ClearId: "cl1", severity: 0, timestamp: 1
 * ClearId: "cl1", severity: 0, timestamp: 2
 * ClearId: "cl1", severity: 4, timestamp: 3  # flap 1
 * ClearId: "cl1", severity: 4, timestamp: 4
 * ClearId: "cl1", severity: 4, timestamp: 5
 * ClearId: "cl1", severity: 0, timestamp: 6
 * ClearId: "cl1", severity: 5, timestamp: 7  # flap 2
 * ClearId: "cl1", severity: 5, timestamp: 8
 * ClearId: "cl1", severity: 0, timestamp: 9
 * ClearId: "cl1", severity: 4, timestamp: 10 # flap 3 (alert sent)
 * ClearId: "cl1", severity: 0, timestamp: 11
 * ClearId: "cl1", severity: 5, timestamp: 12 # flap 4
 * 
 * The hashmap where we keep track of the flapping would look like this after each event:
 * 
 * Event =>  ClearId: "cl1", severity: 0, timestamp: 1
 * HashMap => {"cl1": {lastseverity:0,
 * transitions: []}}
 * 
 * Event =>  ClearId: "cl1", severity: 0, timestamp: 2
 * HashMap => {"cl1": {lastseverity:0,
 * transitions: []}}
 * 
 * Event =>  ClearId: "cl1", severity: 4, timestamp: 3  # flap 1
 * HashMap => {"cl1": {lastseverity:4,
 * transitions: [3]}}
 * 
 * Event =>  ClearId: "cl1", severity: 4, timestamp: 4
 * HashMap => {"cl1": {lastseverity:4,
 * transitions: [3]}}
 * 
 * Event =>  ClearId: "cl1", severity: 4, timestamp: 5
 * HashMap => {"cl1": {lastseverity:4,
 * transitions: [3]}}
 * 
 * Event =>  ClearId: "cl1", severity: 0, timestamp: 6
 * HashMap => {"cl1": {lastseverity:0,
 * transitions: [3]}}
 * 
 * Event =>  ClearId: "cl1", severity: 5, timestamp: 7  # flap 2
 * HashMap => {"cl1": {lastseverity:5,
 * transitions: [3, 7]}}
 * 
 * Event =>  ClearId: "cl1", severity: 5, timestamp: 8
 * HashMap => {"cl1": {lastseverity:5,
 * transitions: [3, 7]}}
 * 
 * Event =>  ClearId: "cl1", severity: 0, timestamp: 9
 * HashMap => {"cl1": {lastseverity:0,
 * transitions: [3, 7]}}
 * 
 * Event =>  ClearId: "cl1", severity: 4, timestamp: 10 # flap 3 (alert sent)
 * HashMap => {"cl1": {lastseverity:5,
 * transitions: [3, 7, 10]}}
 * 
 * Event =>  ClearId: "cl1", severity: 0, timestamp: 11
 * HashMap => {"cl1": {lastseverity:0,
 * transitions: []}}
 * 
 * Event =>  ClearId: "cl1", severity: 5, timestamp: 12
 * HashMap => {"cl1": {lastseverity:0,
 * transitions: [12]}}
 **/
public class EventFlappingPlugin extends EventPreCreatePlugin implements ApplicationListener<ZepEvent> {

    private static final Logger logger = LoggerFactory.getLogger(EventFlappingPlugin.class);
    // spring stuff
    @Autowired
    private FlapTrackerDao flapTrackerDao;
    private Zep.ZepConfig config;
    @Autowired
    private EventPublisher publisher;
    @Autowired
    private UUIDGenerator uuidGenerator;

    // these are overwritten in the config, see the loadConfig method
    private boolean enabled = true;
    private String eventFlappingClass = "/Status/Flapping";


    public void setFlapTrackerDao(FlapTrackerDao s) {
        this.flapTrackerDao = s;
    }

    public void setPublisher(EventPublisher pub) {
        this.publisher = pub;
    }

    public void setConfig(ConfigDao config) {
        try{
            this.config = config.getConfig();
        } catch(ZepException e) {
            logger.warn("Unable to load event flapping configuration", e);
        }
    }

    public void setUuidGenerator(UUIDGenerator uuidGenerator) {
        this.uuidGenerator = uuidGenerator;
    }

    @Override
    public void start(Map<String, String> properties) {
        super.start(properties);
        loadConfig();
    }

    private void loadConfig() {
        logger.info("Event flapping detection plugin loading configuration");
        // update our variables from the config
        enabled = config.getEnableEventFlappingDetection();
        eventFlappingClass = config.getFlappingEventClass();
    }

    @Override
    public void onApplicationEvent(ZepEvent event) {
        if (event instanceof ZepConfigUpdatedEvent) {
            ZepConfigUpdatedEvent configUpdatedEvent = (ZepConfigUpdatedEvent) event;
            this.config = configUpdatedEvent.getConfig();
            loadConfig();
        }
    }
    /**
     *
     * This method looks at the previous flaps stored in the tracker
     * and if it exceeds the threshold then the event is flapping and we return true.
     * @param tracker
     * @param event
     * @param flappingThreshold
     * @return boolean if the event is considered in a state of flapping
     */
    protected boolean shouldGenerateFlapEvent(FlapTracker tracker,Event event, int flappingThreshold) {
        EventSeverity sev = event.getSeverity();
        EventSeverity previousSeverity = tracker.getPreviousSeverity();
        // if the severity hasn't changed it can't be flapping
        if (sev.equals(previousSeverity)) {
            return false;
        }

        // check our amount of flaps
        Long[] timestamps = tracker.getTimestamps();
        int count = 0;
        final long windowStart = System.currentTimeMillis() / 1000l - event.getFlappingIntervalSeconds();
        // get all the timestamps that fall within the window
        for (Long t : timestamps) {
            if (t >= windowStart) {
                count++;
            }
        }

        return count >= flappingThreshold;
    }

    /**
     * Determines if the given event is a flap by looking at the severity of the previous
     * event.
     * @param event
     * @param tracker
     * @param sevThreshold
     * @return boolean if this particular occurrence of an event is considered a flap
     */
    protected boolean isEventFlap(Event event, FlapTracker tracker, EventSeverity sevThreshold) {
        // make sure we are identified before saying this is a flap
        String uuid = event.getActor().getElementUuid();
        if (uuid == null || uuid.equals("")) {
            return false;
        }

        EventSeverity sev = event.getSeverity();
        EventSeverity previousSeverity = tracker.getPreviousSeverity();

        // if the previous severity was less than the severity threshold
        if (previousSeverity.getNumber() < sevThreshold.getNumber() &&
            sev.getNumber() >= sevThreshold.getNumber()) {
            return true;
        }
        return false;
    }

    /**
     * This builds the event flapping event that is emitted when a device
     * is determined to be flapping
     * @param event event that triggered the flapping event
     * @return Event The Flapping Event
     */
    protected Event buildFlappingEvent(Event event) {
        // create an event based off of the passed in event
        // but with the event class from the config
        final Event.Builder flapEvent = Event.newBuilder();
        flapEvent.setUuid(this.uuidGenerator.generate().toString());
        flapEvent.setCreatedTime(System.currentTimeMillis());
        flapEvent.setActor(event.getActor());
        flapEvent.setSummary("Event flapping detected for " + event.getActor().getElementIdentifier());
        flapEvent.setSeverity(EventSeverity.SEVERITY_WARNING);
        flapEvent.setEventClass(eventFlappingClass);
        return flapEvent.build();

    }

    @Override
    public Event processEvent(Event event, EventPreCreateContext context) throws ZepException {
        if (enabled) {
            final long startTime = System.currentTimeMillis();
            // verify that the plugin is enabled
            String fingerprintHash = EventDaoUtils.DEFAULT_GENERATOR.generateClearFingerprint(event);
            if (fingerprintHash != null) {
                detectEventFlapping(event, fingerprintHash);
            }
            final long endTime = System.currentTimeMillis();
            logger.debug("Detected flapping in {} milliseconds", endTime - startTime);
        }
        return event;
    }


    /**
     * First determines if this event is a flap and if it is we determine if we have
     * flapped enough to warrant sending a flap event. This method will update the
     * timestamp in the tracker as well as cull the previous timestamps and publish the
     * flapping event.
     * @param event Event we are detecting flapping on
     * @param fingerprintHash clear finger print hash for this event
     */
    protected void detectEventFlapping(Event event, String fingerprintHash) {
        // verify that the plugin is enabled
        EventSeverity sev = event.getSeverity();
        EventSeverity severityThreshold = event.getFlappingSeverity();
        final int flapThreshold = event.getFlappingThreshold();
        final int flapWindowSeconds = event.getFlappingIntervalSeconds();

        FlapTracker tracker;
        try{
            tracker = flapTrackerDao.getFlapTrackerByClearFingerprintHash(fingerprintHash);
        }catch (ZepException e) {
            logger.warn("Unable to detect event flapping", e);
            return;
        }

        if (isEventFlap(event, tracker, severityThreshold)) {
            // append to our list of flaps
            tracker.addCurrentTimeStamp();

            // see if we have gone above the threshold
            if (shouldGenerateFlapEvent(tracker, event, flapThreshold)) {
                tracker.clearTimestamps();
                logger.info("Publishing flap event for clear {}", fingerprintHash);
                Event flapEvent = buildFlappingEvent(event);
                try {
                    logger.debug("Publishing this event {}", flapEvent);
                    publisher.publishEvent(flapEvent);
                } catch (ZepException e) {
                    logger.error("Unable to publish flap event ", e);
                }
            }
        }
        // make sure we don't persist timestamps that occurred before our window
        long windowStart = System.currentTimeMillis() / 1000l - flapWindowSeconds;
        tracker.discardTimestampsOlderThan(windowStart);
        tracker.setPreviousSeverity(sev);
        try {
            flapTrackerDao.persistTracker(fingerprintHash, tracker, flapWindowSeconds);
        } catch (ZepException e) {
            logger.warn("Unable to detect event flapping", e);
        }
    }
}
