/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.index.impl;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WhitespaceTokenizer;

import java.io.IOException;
import java.io.Reader;

/**
 * Analyzer used for event summaries and messages. Uses combination of lower case filter and
 * whitespace tokenizer.
 */
public final class SummaryAnalyzer extends Analyzer {

    @Override
    public TokenStream tokenStream(String fieldName, Reader reader) {
        TokenStream tokenStream = new WhitespaceTokenizer(IndexConstants.LUCENE_VERSION, reader);
        tokenStream = new LowerCaseFilter(IndexConstants.LUCENE_VERSION, tokenStream);
        return tokenStream;
    }

    private static class SavedStreams {
        Tokenizer source;
        TokenStream result;
    }

    @Override
    public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
        SavedStreams streams = (SavedStreams) getPreviousTokenStream();
        if (streams == null) {
            streams = new SavedStreams();
            streams.source = new WhitespaceTokenizer(IndexConstants.LUCENE_VERSION, reader);
            streams.result = new LowerCaseFilter(IndexConstants.LUCENE_VERSION, streams.source);
            setPreviousTokenStream(streams);
        } else {
            streams.source.reset(reader);
        }
        return streams.result;
    }
}
