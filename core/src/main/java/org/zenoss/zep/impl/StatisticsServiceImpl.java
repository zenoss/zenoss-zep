/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2012, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.ZepConfig;
import org.zenoss.zep.Counters;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.ZepMXBean;
import org.zenoss.zep.StatisticsService;
import org.zenoss.zep.dao.ConfigDao;
import org.zenoss.zep.dao.EventIndexQueueDao;
import org.zenoss.zep.dao.EventSummaryDao;
import org.zenoss.zep.index.EventIndexDao;

import javax.management.MBeanAttributeInfo;
import javax.management.StandardMBean;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StatisticsServiceImpl extends StandardMBean implements StatisticsService {

    private static final Logger logger = LoggerFactory.getLogger(StatisticsServiceImpl.class);

    private static final Map<String, String> attributeDescriptions = new HashMap<String, String>();
    static {
        attributeDescriptions.put("ProcessedEventCount", "processed events");
        attributeDescriptions.put("DedupedEventCount", "deduped events");
        attributeDescriptions.put("EventCount", "processed events");
        attributeDescriptions.put("DroppedEventCount", "dropped events");
        attributeDescriptions.put("ClearedEventCount", "cleared events");
        attributeDescriptions.put("ArchivedEventCount", "archived events");
        attributeDescriptions.put("AgedEventCount", "aged events");
        attributeDescriptions.put("ArchiveEligibleEventCount", "archive-eligible events");
        attributeDescriptions.put("AgeEligibleEventCount", "age-eligible events");
        attributeDescriptions.put("SummaryQueueLength", "summary queue length");
        attributeDescriptions.put("ArchiveQueueLength", "archive queue length");
        attributeDescriptions.put("SummaryIndexSize", "summary index size");
        attributeDescriptions.put("ArchiveIndexSize", "archive index size");
        attributeDescriptions.put("SummaryIndexDocCount", "summary index doc count");
        attributeDescriptions.put("ArchiveIndexDocCount", "archive index doc count");
    }

    private Counters counters;
    private ConfigDao configDao;
    private EventSummaryDao eventSummaryDao;
    private EventIndexDao eventSummaryIndexDao;
    private EventIndexDao eventArchiveIndexDao;
    private EventIndexQueueDao eventSummaryIndexQueueDao;
    private EventIndexQueueDao eventArchiveIndexQueueDao;

    public StatisticsServiceImpl() {
        super(ZepMXBean.class, true);
    }

    @Override
    protected String getDescription(MBeanAttributeInfo info) {
        return attributeDescriptions.get(info.getName());
    }

    @Override
    public String getAttributeDescription(String name) {
        return attributeDescriptions.get(name);
    }

    public void setCounters(final Counters counters) {
        this.counters = counters;
    }

    public void setConfigDao(final ConfigDao configDao) {
        this.configDao = configDao;
    }

    public void setEventSummaryDao(final EventSummaryDao eventSummaryDao) {
        this.eventSummaryDao = eventSummaryDao;
    }

    public void setEventSummaryIndexDao(final EventIndexDao eventSummaryIndexDao) {
        this.eventSummaryIndexDao = eventSummaryIndexDao;
    }

    public void setEventArchiveIndexDao(final EventIndexDao eventArchiveIndexDao) {
        this.eventArchiveIndexDao = eventArchiveIndexDao;
    }

    public void setEventSummaryIndexQueueDao(final EventIndexQueueDao eventSummaryIndexQueueDao) {
        this.eventSummaryIndexQueueDao = eventSummaryIndexQueueDao;
    }

    public void setEventArchiveIndexQueueDao(final EventIndexQueueDao eventArchiveIndexQueueDao) {
        this.eventArchiveIndexQueueDao = eventArchiveIndexQueueDao;
    }

    @Override
    public long getAgedEventCount() {
        return counters.getAgedEventCount();
    }

    @Override
    public long getArchivedEventCount() {
        return counters.getArchivedEventCount();
    }

    @Override
    public long getClearedEventCount() {
        return counters.getClearedEventCount();
    }

    @Override
    public long getDedupedEventCount() {
        return counters.getDedupedEventCount();
    }

    @Override
    public long getDroppedEventCount() {
        return counters.getDroppedEventCount();
    }

    @Override
    public long getProcessedEventCount() {
        return counters.getProcessedEventCount();
    }

    @Override
    public long getArchiveEligibleEventCount() {
        try {
            long duration = configDao.getConfig().getEventArchiveIntervalMinutes();
            return eventSummaryDao.getArchiveEligibleEventCount(duration, TimeUnit.MINUTES);
        } catch (ZepException e) {
            logger.warn("Cannot get configuration.");
            return -1;
        }
    }

    @Override
    public long getAgeEligibleEventCount() {
        try {
            ZepConfig config = configDao.getConfig();
            long duration = config.getEventAgeIntervalMinutes();
            EventSeverity maxSeverity = config.getEventAgeDisableSeverity();
            boolean inclusiveSeverity = config.getEventAgeSeverityInclusive();
            return eventSummaryDao.getAgeEligibleEventCount(duration, TimeUnit.MINUTES, maxSeverity, inclusiveSeverity);
        } catch (ZepException e) {
            logger.warn("Cannot get configuration.");
            return -1;
        }
    }

    @Override
    public long getSummaryQueueLength() {
        return eventSummaryIndexQueueDao.getQueueLength();
    }

    @Override
    public long getArchiveQueueLength() {
        return eventArchiveIndexQueueDao.getQueueLength();
    }

    @Override
    public long getSummaryIndexSize() {
        return eventSummaryIndexDao.getSize();
    }

    @Override
    public long getArchiveIndexSize() {
        return eventArchiveIndexDao.getSize();
    }

    private static long getNumDocs(EventIndexDao eventIndexDao) {
        try {
            return eventIndexDao.getNumDocs();
        } catch (ZepException e) {
            logger.warn(String.format("Cannot get number of docs in index %s", eventIndexDao.getName()));
            return -1;
        }
    }

    @Override
    public long getSummaryIndexDocCount() {
        return getNumDocs(eventSummaryIndexDao);
    }

    @Override
    public long getArchiveIndexDocCount() {
        return getNumDocs(eventArchiveIndexDao);
    }
}
