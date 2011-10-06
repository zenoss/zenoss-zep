/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
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
    public <T> T executeInNestedTransaction(NestedTransactionCallback<T> callback) throws DataAccessException;
}
