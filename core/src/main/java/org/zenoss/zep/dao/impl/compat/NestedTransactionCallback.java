/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl.compat;

import org.springframework.dao.DataAccessException;

/**
 * Callback interface used to perform operations in a nested transaction.
 */
public interface NestedTransactionCallback<T> {
    /**
     * Performs an operation in a nested transaction using the specified context.
     *
     * @param context Context passed to nested transaction callback.
     * @return Return type of operation.
     * @throws DataAccessException If an exception occurs.
     */
    T doInNestedTransaction(NestedTransactionContext context) throws DataAccessException;
}
