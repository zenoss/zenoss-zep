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

import org.apache.lucene.analysis.CharTokenizer;
import org.apache.lucene.util.AttributeSource;

import java.io.Reader;

/**
 * Tokenizer for element identifiers and sub identifiers.
 */
public class IdentifierTokenizer extends CharTokenizer {

    public IdentifierTokenizer(Reader input) {
        super(input);
    }

    public IdentifierTokenizer(AttributeSource source, Reader input) {
        super(source, input);
    }

    public IdentifierTokenizer(AttributeFactory factory, Reader input) {
        super(factory, input);
    }

    @Override
    protected boolean isTokenChar(char c) {
        return !(Character.isWhitespace(c) || c == '.' || c == '-');
    }
}
