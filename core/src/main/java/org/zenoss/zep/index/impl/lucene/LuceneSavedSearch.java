/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.index.impl.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.zenoss.zep.index.impl.SavedSearch;

import java.io.IOException;

public final class LuceneSavedSearch extends SavedSearch {
    private IndexReader reader;
    private final Query query;
    private final Sort sort;

    public LuceneSavedSearch(String uuid, IndexReader reader, Query query, Sort sort, int timeout) {
        super(uuid, timeout);
        this.reader = reader;
        this.query = query;
        this.sort = sort;
    }

    public IndexReader getReader() {
        return this.reader;
    }

    public Query getQuery() {
        return this.query;
    }

    public Sort getSort() {
        return sort;
    }

    public synchronized void close() throws IOException {
        if (this.reader != null) {
            this.reader.decRef();
            this.reader = null;
        }
    }

    @Override
    public String toString() {
        return "LuceneSavedSearch{uuid=" + getUuid() +
                ", reader=" + reader +
                ", query=" + query +
                ", sort=" + sort +
                ", timeout=" + getTimeout() +
                '}';
    }
}
