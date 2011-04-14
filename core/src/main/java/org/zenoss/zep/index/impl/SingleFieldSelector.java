/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
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
