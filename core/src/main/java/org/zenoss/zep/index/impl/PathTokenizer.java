/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.index.impl;

import org.apache.lucene.analysis.CharTokenizer;

import java.io.Reader;

/**
 * Tokenizer for event classes (splits on slash characters).
 */
public class PathTokenizer extends CharTokenizer {
    public PathTokenizer(Reader input) {
        super(IndexConstants.LUCENE_VERSION, input);
    }

    @Override
    protected boolean isTokenChar(int c) {
        return (c != '/');
    }
}
