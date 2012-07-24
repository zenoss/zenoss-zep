/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2012, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep;

/**
 * ZEP provides a JMX ZepMXBean for key performance statistics, including:
 *   Total processed events (since last restart)
 *   Number of de-duped events
 *   Number of dropped events
 *   Number of cleared events
 *   Number of archived events
 *   Number of aged events
 *   Number of events that could be archived
 *   Number of events that could be aged
 *   Index summary queue table count (SELECT COUNT(*) FROM event_summary_index_queue)
 *   Index archive queue table count (SELECT COUNT(*) FROM event_archive_index_queue)
 *   relevant Lucene statistics. Size of indexes.
 */
public interface ZepMXBean {
    public long getProcessedEventCount();
    public long getDedupedEventCount();
    public long getDroppedEventCount();
    public long getClearedEventCount();
    public long getArchivedEventCount();
    public long getAgedEventCount();
    public long getArchiveEligibleEventCount();
    public long getAgeEligibleEventCount();
    public long getSummaryQueueLength();
    public long getArchiveQueueLength();
    public long getSummaryIndexSize();
    public long getArchiveIndexSize();
    public long getSummaryIndexDocCount();
    public long getArchiveIndexDocCount();
}
