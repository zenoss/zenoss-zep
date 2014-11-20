/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/
package org.zenoss.zep.index.impl;

import java.io.IOException;

public class MultiBackendSavedSearch extends SavedSearch {

    public final String backendId;
    public final String savedSearchId;
    private final MultiBackendEventIndexDao dao;

    public MultiBackendSavedSearch(String uuid, int timeout, String backendId, String savedSearchId, MultiBackendEventIndexDao dao) {
        super(uuid, timeout);
        this.backendId = backendId;
        this.savedSearchId = savedSearchId;
        this.dao = dao;
    }

    @Override
    public void close() throws IOException {
        dao.closeBackendSavedSearch(backendId, savedSearchId);
    }
}
