package org.zenoss.zep.index;

import org.zenoss.zep.ZepException;

public interface LuceneEventIndexDao extends EventIndexDao {

    /**
     * Sets the reader reopen interval on the NRTManagerReopenThread
     *
     * @param interval Interval (in seconds) to set the reader reopen interval to
     * @throws ZepException If the interval cannot be set
     */
    void setReaderReopenInterval(int interval) throws ZepException;

}
