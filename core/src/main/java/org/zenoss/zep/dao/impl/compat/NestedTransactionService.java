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
 * Service used to perform operations in nested transactions.
 */
public interface NestedTransactionService {
    /**
     * Method used to perform an operation (via the specified callback) in a nested transaction. Creating the
     * transaction and performing rollbacks is performed automatically.
     *
     * @param callback Callback to perform in a nested transaction.
     * @param <T> Return type of callback.
     * @return The return value of the callback operation.
     * @throws DataAccessException If an exception occurs.
     */
    <T> T executeInNestedTransaction(NestedTransactionCallback<T> callback) throws DataAccessException;
}
