/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.index.impl.solr;

import org.apache.solr.client.solrj.SolrQuery;
import org.zenoss.zep.index.impl.SavedSearch;

import java.io.IOException;

public final class SolrSavedSearch extends SavedSearch {

    private final SolrQuery solrQuery;

    public SolrSavedSearch(String uuid, int timeout, SolrQuery solrQuery) {
        super(uuid, timeout);
        this.solrQuery = solrQuery;
    }

    public SolrQuery getSolrQuery() {
        return solrQuery;
    }

    @Override
    public final void close() throws IOException {
        // do nothing
    }
}
