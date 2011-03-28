/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2011, Zenoss Inc.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 *
 * For complete information please visit: http://www.zenoss.com/oss/
 */
package org.zenoss.zep.index.impl;

import org.apache.lucene.analysis.CharTokenizer;

import java.io.Reader;

/**
 * Tokenizer for event classes (splits on slash characters).
 */
public class EventClassTokenizer extends CharTokenizer {
    public EventClassTokenizer(Reader input) {
        super(input);
    }

    @Override
    protected boolean isTokenChar(char c) {
        return (c != '/');
    }
}