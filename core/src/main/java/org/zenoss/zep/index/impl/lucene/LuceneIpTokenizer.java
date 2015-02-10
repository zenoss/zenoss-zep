/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2011, 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/


package org.zenoss.zep.index.impl.lucene;

import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.util.AttributeSource;
import org.zenoss.zep.index.impl.IndexConstants;

import java.io.Reader;

/**
 * Tokenizer for IP addresses.
 */
public class LuceneIpTokenizer extends CharTokenizer {
    /**
     * {@inheritDoc}
     */
    public LuceneIpTokenizer(Reader input) {
        super(IndexConstants.LUCENE_VERSION, input);
    }

    /**
     * {@inheritDoc}
     */
    public LuceneIpTokenizer(AttributeSource source, Reader input) {
        super(IndexConstants.LUCENE_VERSION, source.getAttributeFactory(), input);
    }

    /**
     * {@inheritDoc}
     */
    public LuceneIpTokenizer(AttributeSource.AttributeFactory factory, Reader input) {
        super(IndexConstants.LUCENE_VERSION, factory, input);
    }

    @Override
    protected boolean isTokenChar(int c) {
        return (c != '.' && c != ':');
    }
}
