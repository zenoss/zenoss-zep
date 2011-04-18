/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.index.impl;

import org.apache.lucene.analysis.CharTokenizer;
import org.apache.lucene.util.AttributeSource;

import java.io.Reader;

/**
 * Tokenizer for IP addresses.
 */
public class IpTokenizer extends CharTokenizer {
    /**
     * {@inheritDoc}
     */
    public IpTokenizer(Reader input) {
        super(input);
    }

    /**
     * {@inheritDoc}
     */
    public IpTokenizer(AttributeSource source, Reader input) {
        super(source, input);
    }

    /**
     * {@inheritDoc}
     */
    public IpTokenizer(AttributeFactory factory, Reader input) {
        super(factory, input);
    }

    @Override
    protected boolean isTokenChar(char c) {
        return (c != '.' && c != ':');
    }
}
