/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl.compat;

import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;

/**
 * Interface representing the context available to a nested transaction callback.
 *
 * @see NestedTransactionCallback
 */
public interface NestedTransactionContext {
    public SimpleJdbcTemplate getSimpleJdbcTemplate();
}
