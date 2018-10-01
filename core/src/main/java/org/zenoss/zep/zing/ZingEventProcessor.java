
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
