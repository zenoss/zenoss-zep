package org.zenoss.zep;

import javax.management.DynamicMBean;

public interface StatisticsService extends ZepMXBean, DynamicMBean {

    public void addToProcessedEventCount(long delta);
    public void addToDedupedEventCount(long delta);
    public void addToDroppedEventCount(long delta);
    public void addToClearedEventCount(long delta);
    public void addToArchivedEventCount(long delta);
    public void addToAgedEventCount(long delta);

    public String getAttributeDescription(String attributeName);

}
