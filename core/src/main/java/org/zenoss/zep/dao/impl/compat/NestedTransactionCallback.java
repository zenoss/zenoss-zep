/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
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
    public T doInNestedTransaction(NestedTransactionContext context) throws DataAccessException;
}
