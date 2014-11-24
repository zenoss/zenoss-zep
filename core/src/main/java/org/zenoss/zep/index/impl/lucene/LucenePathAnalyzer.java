/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2010, 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/


package org.zenoss.zep.index.impl.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.zenoss.zep.index.impl.IndexConstants;

import java.io.Reader;

/**
 * Analyzer used for storing event classes.
 */
public final class LucenePathAnalyzer extends Analyzer {

    @Override
    protected TokenStreamComponents createComponents(String s, Reader reader) {
        final Tokenizer source = new LucenePathTokenizer(reader);
        TokenStream filter = new LowerCaseFilter(IndexConstants.LUCENE_VERSION, source);
        return new TokenStreamComponents(source, filter);
    }
}
