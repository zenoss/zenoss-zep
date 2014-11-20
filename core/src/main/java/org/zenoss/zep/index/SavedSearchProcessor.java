package org.zenoss.zep.index;

import org.zenoss.protobufs.zep.Zep.EventQuery;
import org.zenoss.protobufs.zep.Zep.EventSummaryResult;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.index.impl.BaseEventIndexBackend;
import org.zenoss.zep.index.impl.SavedSearch;

public interface SavedSearchProcessor<SS extends SavedSearch> {
    /**
     * Execute a saved search and return limit results at the specified offset.
     *
     * @param search The saved search (returned from {@link BaseEventIndexBackend#buildSavedSearch(String, EventQuery)} .
     * @param offset Offset within the search to return.
     * @param limit Number of results to return.
     * @return The result of the search.
     * @throws ZepException If an exception occurs performing the saved query.
     */
    public EventSummaryResult result(SS search, int offset, int limit) throws ZepException;
}
