/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


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
