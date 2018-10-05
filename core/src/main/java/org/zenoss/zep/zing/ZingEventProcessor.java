/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2018, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.zing;


import org.zenoss.protobufs.zep.Zep.EventSummary;


public interface ZingEventProcessor {

    /**
     *
     * @return boolean indicating if events are forwarded to Zenoss cloud
     */
    boolean enabled();

    /**
     *
     * @param eventSummary event to forward to Zenoss Cloud
     */
    void processEvent(EventSummary eventSummary);

}
