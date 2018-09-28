
package org.zenoss.zep.zing;

import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.Event;

import java.util.List;

public interface ZingEventProcessor {

    /**
     * Called every time a new occurrence of an event happens
     * @param eventOccurrence
     * @param eventSummary
     */
    void processEvent(Event eventOccurrence, EventSummary eventSummary);

    /**
     * Called when the event metadata has changed. This will only update the
     * event's metadata in Zenoss Cloud and not the event's time series
     * @param uuids
     */
    void processUpdatedEvents(List<String> uuids);

}
