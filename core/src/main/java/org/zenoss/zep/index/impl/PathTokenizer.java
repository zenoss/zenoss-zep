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
        super(input);
    }

    @Override
    protected boolean isTokenChar(char c) {
        return (c != '/');
    }
}