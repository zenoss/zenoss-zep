/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2010, 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/


package org.zenoss.zep.index.impl.lucene;

import org.apache.lucene.analysis.util.CharTokenizer;
import org.zenoss.zep.index.impl.IndexConstants;

import java.io.Reader;

/**
 * Tokenizer for event classes (splits on slash characters).
 */
public class LucenePathTokenizer extends CharTokenizer {
    public LucenePathTokenizer(Reader input) {
        super(IndexConstants.LUCENE_VERSION, input);
    }

    @Override
    protected boolean isTokenChar(int c) {
        return (c != '/');
    }
}
