
package org.zenoss.zep.zing;

import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.Event;

public interface ZingEventProcessor {

    public void processEvent(Event eventOccurrence, EventSummary eventSummary);

}
