/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao.impl.compat;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;

/**
 * Implemented of NestedTransactionService.
 */
public class NestedTransactionServiceImpl implements NestedTransactionService {

    private final SimpleJdbcTemplate template;
    private final NestedTransactionContext context;

    public NestedTransactionServiceImpl(DataSource dataSource) {
        this.template = new SimpleJdbcTemplate(dataSource);
        this.context = new NestedTransactionContext() {
            @Override
            public SimpleJdbcTemplate getSimpleJdbcTemplate() {
                return template;
            }
        };
    }

    @Override
    @Transactional(propagation = Propagation.NESTED)
    public <T> T executeInNestedTransaction(final NestedTransactionCallback<T> callback) throws DataAccessException {
        return callback.doInNestedTransaction(this.context);
    }
}
