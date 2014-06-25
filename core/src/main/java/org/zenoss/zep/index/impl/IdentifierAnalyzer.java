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
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.util.Version;

import java.io.Reader;

/**
 * Analyzer used for element and sub element identifiers.
 */
public final class IdentifierAnalyzer extends Analyzer {

    public static final int MIN_NGRAM_SIZE = 3;
    public static final int MAX_NGRAM_SIZE = MIN_NGRAM_SIZE;

    @Override
    protected TokenStreamComponents createComponents(String s, Reader reader) {
        final Tokenizer source = new WhitespaceTokenizer(IndexConstants.LUCENE_VERSION, reader);
        TokenStream filter = new LowerCaseFilter(IndexConstants.LUCENE_VERSION, source);
        // Use the 4.3 NGram filter here because it changed a lot >=4.4
        filter = new NGramTokenFilter(Version.LUCENE_43, filter, MIN_NGRAM_SIZE, MAX_NGRAM_SIZE);
        return new TokenStreamComponents(source, filter);
    }

}

