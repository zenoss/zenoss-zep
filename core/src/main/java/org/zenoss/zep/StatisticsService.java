/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2012, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

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
