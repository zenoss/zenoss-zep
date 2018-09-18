
package org.zenoss.zep.zing;

import org.zenoss.protobufs.zep.Zep.EventSummary;

public interface ZingEventProcessor {

    public void processEvent(EventSummary eventSummary);

}
