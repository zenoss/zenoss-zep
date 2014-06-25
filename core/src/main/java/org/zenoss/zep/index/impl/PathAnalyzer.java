/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/


package org.zenoss.zep.index.impl;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

import java.io.Reader;

/**
 * Analyzer used for storing event classes.
 */
public final class PathAnalyzer extends Analyzer {

    @Override
    protected TokenStreamComponents createComponents(String s, Reader reader) {
        final Tokenizer source = new PathTokenizer(reader);
        TokenStream filter = new LowerCaseFilter(IndexConstants.LUCENE_VERSION, source);
        return new TokenStreamComponents(source, filter);
    }

}
