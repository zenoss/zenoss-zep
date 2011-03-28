/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2010, Zenoss Inc.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 *
 * For complete information please visit: http://www.zenoss.com/oss/
 */
package org.zenoss.zep.index.impl;

import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;

/**
 * FieldSelector used to select a single field from a document.
 */
public class SingleFieldSelector implements FieldSelector {
    private final String fieldName;

    public SingleFieldSelector(String fieldName) {
        if (fieldName == null) {
            throw new NullPointerException();
        }
        this.fieldName = fieldName;
    }

    @Override
    public FieldSelectorResult accept(String fieldName) {
        return (this.fieldName.equals(fieldName)) ? FieldSelectorResult.LOAD_AND_BREAK : FieldSelectorResult.NO_LOAD;
    }

    @Override
    public String toString() {
        return "SingleFieldSelector{" +
                "fieldName='" + fieldName + '\'' +
                '}';
    }
}
