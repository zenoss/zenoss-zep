/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2010, 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/


package org.zenoss.zep.index.impl.lucene;

import com.google.protobuf.ProtocolMessageEnum;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.springframework.util.StringUtils;
import org.zenoss.protobufs.util.Util.TimestampRange;
import org.zenoss.protobufs.zep.Zep.EventDetailItem;
import org.zenoss.protobufs.zep.Zep.EventDetailItem.EventDetailType;
import org.zenoss.protobufs.zep.Zep.FilterOperator;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.index.IndexedDetailsConfiguration;
import org.zenoss.zep.index.impl.BaseQueryBuilder;
import org.zenoss.zep.index.impl.IndexConstants;
import org.zenoss.zep.utils.IpRange;
import org.zenoss.zep.utils.IpUtils;

import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.util.*;
import java.util.Map.Entry;

import static org.zenoss.zep.index.impl.lucene.LuceneFilterCacheManager.FilterType.*;

public class LuceneQueryBuilder extends BaseQueryBuilder<LuceneQueryBuilder> {
    private final IndexedDetailsConfiguration indexedDetailsConfiguration;
    private final LuceneFilterCacheManager filterCache;
    private final IndexReader reader;

    public LuceneQueryBuilder(LuceneFilterCacheManager fcm, IndexReader reader,
                              IndexedDetailsConfiguration configuration) {
        this(Occur.MUST, fcm, reader, configuration);
    }

    public LuceneQueryBuilder(FilterOperator op, LuceneFilterCacheManager fcm, IndexReader reader,
                              IndexedDetailsConfiguration configuration) {
        this(op == FilterOperator.OR ? Occur.SHOULD : Occur.MUST, fcm, reader, configuration);
    }

    private LuceneQueryBuilder(Occur occur, LuceneFilterCacheManager fcm, IndexReader reader,
                               IndexedDetailsConfiguration configuration) {
        super(occur);
        this.filterCache = fcm;
        this.reader = reader;
        this.indexedDetailsConfiguration = configuration;
    }

    @Override
    protected LuceneQueryBuilder subBuilder(Occur occur) {
        return new LuceneQueryBuilder(occur, filterCache, reader, indexedDetailsConfiguration);
    }

    @Override
    protected EventDetailType getDetailType(String key) throws ZepException {
        EventDetailItem item = indexedDetailsConfiguration.getEventDetailItemsByName().get(key);
        return (item == null) ? null : item.getType();
    }

    @Override
    protected String getDetailKey(String key) throws ZepException {
        EventDetailItem item = indexedDetailsConfiguration.getEventDetailItemsByName().get(key);
        return (item == null) ? null : LuceneEventIndexMapper.DETAIL_INDEX_PREFIX + item.getKey();
    }

    public BooleanQuery build() throws ZepException {
        Filter filter = buildFilter();
        if (filter == null) return null;
        BooleanQuery query = new BooleanQuery();
        query.add(new FilteredQuery(new MatchAllDocsQuery(), filter), BooleanClause.Occur.MUST);
        return query;
    }

    @SuppressWarnings("unchecked")
    private Filter buildFilter() throws ZepException {
        final List<Filter> filters = new ArrayList<Filter>();

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.DATE_RANGE))
            buildDateRangeFilters(filters, field.getKey(), (Set<TimestampRange>) field.getValue());

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.ENUM_NUMBER))
            buildEnumNumberFilters(filters, field.getKey(), (Set<ProtocolMessageEnum>) field.getValue());

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.FULL_TEXT))
            buildFullTextFilters(filters, field.getKey(), (Set<String>) field.getValue());

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.IDENTIFIER))
            buildIdentifierFilters(filters, field.getKey(), (Set<String>) field.getValue());

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.IP_ADDRESS_SUBSTRING))
            buildIpAddressSubstringFilters(filters, field.getKey(), (Set<String>) field.getValue());

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.IP_ADDRESS))
            buildIpAddressFilters(filters, field.getKey(), (Set<InetAddress>) field.getValue());

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.IP_ADDRESS_RANGE))
            buildIpAddressRangeFilters(filters, field.getKey(), (Set<IpRange>) field.getValue());

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.NUMERIC_RANGE))
            // Might not be Integers, but Java does type-erasure, so who cares.
            buildNumericRangeFilters(filters, field.getKey(), (Set<Range<Integer>>) field.getValue());

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.PATH))
            buildPathFilters(filters, field.getKey(), (Set<String>) field.getValue());

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.TERM))
            buildTermFilters(filters, field.getKey(), (Set<String>) field.getValue());

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.WILDCARD))
            buildWildcardFilters(filters, field.getKey(), (Set<String>) field.getValue());

        for (LuceneQueryBuilder sub : subClauses) {
            Filter filter = sub.buildFilter();
            if (filter != null)
                filters.add(filter);
        }

        if (filters.isEmpty()) return null;
        BooleanClause.Occur op = (occur == Occur.SHOULD) ? BooleanClause.Occur.SHOULD : BooleanClause.Occur.MUST;
        BooleanFilter booleanFilter = new BooleanFilter();
        for (Filter filter : filters) {
            booleanFilter.add(filter, op);
        }
        return booleanFilter;
    }

    private void buildDateRangeFilters(List<Filter> filters, String fieldName, Set<TimestampRange> ranges) throws ZepException {
        if (ranges == null || ranges.isEmpty()) return;
        for (TimestampRange range : ranges) {
            Long from = null, to = null;
            if (range.hasStartTime()) {
                from = range.getStartTime();
            }
            if (range.hasEndTime()) {
                to = range.getEndTime();
            }
            filters.add(NumericRangeFilter.newLongRange(fieldName, from, to, true, true));
        }
    }

    private void buildEnumNumberFilters(List<Filter> filters, String fieldName, Set<ProtocolMessageEnum> enums) {
        if (enums == null || enums.isEmpty()) return;
        List<Integer> values = new ArrayList<Integer>(enums.size());
        for (ProtocolMessageEnum e : enums)
            values.add(e.getNumber());

        // Condense adjacent values into one range
        Collections.sort(values);
        Iterator<Integer> it = values.iterator();
        int from = it.next();
        int to = from;
        while (it.hasNext()) {
            int value = it.next();
            if (value == to + 1) {
                to = value;
            } else {
                filters.add(NumericRangeFilter.newIntRange(fieldName, from, to, true, true));
                from = to = value;
            }
        }
        filters.add(NumericRangeFilter.newIntRange(fieldName, from, to, true, true));
    }

    private void buildFullTextFilters(List<Filter> filters, String fieldName, Set<String> values) throws ZepException {
        if (values == null || values.isEmpty()) return;

        Analyzer analyzer = new LuceneSummaryAnalyzer();
        BooleanFilter outerFilter = new BooleanFilter(); // SHOULD
        for (String value : values) {
            // If we encounter a term which starts with a quote, start a new multi-phrase query.
            // If we are already in a multi-phrase query, add the term to the query (accounting
            // for prefix queries). When we encounter a term which ends with a quote, end the
            // multi-phrase query. All queries not enclosed in quotes are treated as normal
            // prefix or term queries.
            BooleanFilter innerFilter = new BooleanFilter(); // MUST
            MultiPhraseQuery pq = null;

            List<String> tokens = getTokens(fieldName, analyzer, value);
            for (String token : tokens) {
                final boolean startsWithQuote = token.startsWith("\"");

                if (startsWithQuote && pq == null) {
                    token = token.substring(1);
                    if (token.isEmpty()) {
                        continue;
                    }
                    pq = new MultiPhraseQuery();
                }

                if (pq == null) {
                    // We aren't in quoted string - just add as query on field
                    Term term = new Term(fieldName, token);
                    innerFilter.add(filterCache.get(WILDCARD, term), BooleanClause.Occur.MUST);
                } else {
                    final boolean endsWithQuote = token.endsWith("\"");
                    // Remove trailing quote
                    if (endsWithQuote) {
                        token = token.substring(0, token.length() - 1);
                    }
                    Term[] terms = getMatchingTerms(fieldName, reader, token);
                    // Ensure we don't return results if term doesn't exist
                    if (terms.length == 0) {
                        pq.add(new Term(fieldName, token));
                    } else {
                        pq.add(terms);
                    }
                    if (endsWithQuote) {
                        innerFilter.add(new QueryWrapperFilter(pq), BooleanClause.Occur.MUST);
                        pq = null;
                    }
                }
            }

            // This could happen if phrase query is not terminated above (i.e.
            // live search with query '"quick brown dog'. This shouldn't be an
            // error to allow live searches to work as expected.
            if (pq != null) {
                innerFilter.add(new QueryWrapperFilter(pq), BooleanClause.Occur.MUST);
            }

            int numClauses = innerFilter.clauses().size();
            if (numClauses == 1) {
                outerFilter.add(innerFilter.clauses().get(0).getFilter(), BooleanClause.Occur.SHOULD);
            } else if (numClauses > 1) {
                outerFilter.add(innerFilter, BooleanClause.Occur.SHOULD);
            }
        }
        int numClauses = outerFilter.clauses().size();
        if (numClauses == 0) return;
        if (numClauses == 1 || occur == Occur.SHOULD)
            for (FilterClause fc : outerFilter.clauses())
                filters.add(fc.getFilter());
        else
            filters.add(outerFilter);
    }

    private void buildIdentifierFilters(List<Filter> filters, String fieldName, Set<String> values) throws ZepException {
        if (values == null || values.isEmpty()) return;
        final Analyzer analyzer = new LuceneIdentifierAnalyzer();
        for (String value : values) {
            final Filter filter;
            value = StringUtils.trimTrailingCharacter(value, '*');

            final String unquoted = unquote(value);

            if (value.isEmpty() || !unquoted.equals(value)) {
                filter = filterCache.get(WILDCARD, new Term(nonAnalyzed(fieldName), unquoted.toLowerCase()));
            } else if (value.length() < LuceneIdentifierAnalyzer.MIN_NGRAM_SIZE) {
                filter = filterCache.get(PREFIX, new Term(fieldName, value.toLowerCase()));
            } else {
                // Use NGramPhraseQuery (new in Lucene 3.5 and optimized for searching NGram fields)
                List<String> tokens = getTokens(fieldName, analyzer, value);
                Term[] terms = new Term[tokens.size()];
                int i = 0;
                for (String token : tokens)
                    terms[i++] = new Term(fieldName, token);
                filter = filterCache.get(NGRAM, terms);
            }
            filters.add(filter);
        }
    }

    private void buildIpAddressFilters(List<Filter> filters, String fieldName, Set<InetAddress> values) throws ZepException {
        if (values == null || values.isEmpty()) return;
        Set<String> hosts = new HashSet<String>(values.size());
        for (InetAddress addr : values)
            hosts.add(IpUtils.canonicalIpAddress(addr));
        buildTermFilters(filters, fieldName + "_sort", hosts);
    }

    private void buildIpAddressRangeFilters(List<Filter> filters, String fieldName, Set<IpRange> values) throws ZepException {
        if (values == null || values.isEmpty()) return;
        Set<Range<BytesRef>> ranges = new HashSet<Range<BytesRef>>(values.size());
        for (IpRange range : values) {
            BytesRef from  = new BytesRef(IpUtils.canonicalIpAddress(range.getFrom()));
            BytesRef to = new BytesRef(IpUtils.canonicalIpAddress(range.getTo()));
            filters.add(new TermRangeFilter(fieldName + "_sort", from, to, true, true));
        }
    }

    private void buildIpAddressSubstringFilters(List<Filter> filters, String fieldName, Set<String> values) throws ZepException {
        if (values == null || values.isEmpty()) return;
        for (String val : values) {
            if (val.indexOf('.') >= 0) {
                BooleanFilter bq = new BooleanFilter();
                bq.add(filterCache.get(TERMS, new Term(fieldName + IndexConstants.IP_ADDRESS_TYPE_SUFFIX,
                        IndexConstants.IP_ADDRESS_TYPE_4)), BooleanClause.Occur.MUST);
                String[] tokens = StringUtils.tokenizeToStringArray(val, ".");
                if (tokens.length > 0) {
                    bq.add(new QueryWrapperFilter(createIpAddressMultiPhraseQuery(fieldName, reader, tokens)), BooleanClause.Occur.MUST);
                } else {
                    bq.add(filterCache.get(TERMS, new Term(fieldName, val)), BooleanClause.Occur.MUST);
                }
                filters.add(bq);
            } else if (val.indexOf(':') >= 0) {
                BooleanFilter bq = new BooleanFilter();
                bq.add(filterCache.get(TERMS, new Term(fieldName + IndexConstants.IP_ADDRESS_TYPE_SUFFIX,
                        IndexConstants.IP_ADDRESS_TYPE_6)), BooleanClause.Occur.MUST);
                String[] tokens = StringUtils.tokenizeToStringArray(val, ":");
                if (tokens.length > 0) {
                    bq.add(new QueryWrapperFilter(createIpAddressMultiPhraseQuery(fieldName, reader, tokens)), BooleanClause.Occur.MUST);
                } else {
                    bq.add(filterCache.get(TERMS, new Term(fieldName, val)), BooleanClause.Occur.MUST);
                }
                filters.add(bq);
            } else {
                filters.add(new QueryWrapperFilter(new WildcardQuery(new Term(fieldName, removeLeadingZeros(val)))));
            }
        }
    }

    private <N extends Number & Comparable<N>> void buildNumericRangeFilter(List<Filter> filters, String fieldName, Range<N> range) {
        Object from = range.from; // Make the compiler happy.
        Object to = range.to;     // Make the compiler happy.
        if (from instanceof Integer || to instanceof Integer)
            filters.add(NumericRangeFilter.newIntRange(fieldName, (Integer)from, (Integer)to, true, true));
        else if (from instanceof Long || to instanceof Long)
            filters.add(NumericRangeFilter.newLongRange(fieldName, (Long)from, (Long)to, true, true));
        else if (from instanceof Double || to instanceof Double)
            filters.add(NumericRangeFilter.newDoubleRange(fieldName, (Double)from, (Double)to, true, true));
        else if (from instanceof Float || to instanceof Float)
            filters.add(NumericRangeFilter.newFloatRange(fieldName, (Float)from, (Float)to, true, true));
        else if (from != null || to != null) {
            logger.warn("Unexpected number range types: ({}, {})", from == null ? null : from.getClass(), to == null ? null : to.getClass());
            filters.add(NumericRangeFilter.newDoubleRange(fieldName, range.from == null ? null : range.from.doubleValue(), to == null ? null : range.to.doubleValue(), true, true));
        }
    }

    private <N extends Number & Comparable<N>> void buildNumericRangeFilters(List<Filter> filters, String fieldName, Set<Range<N>> ranges) {
        if (ranges == null || ranges.isEmpty()) return;
        if (occur == Occur.MUST) {
            for (Range<N> range : ranges)
                buildNumericRangeFilter(filters, fieldName, range);
            return;
        }

        List<Range<N>> values = new ArrayList<Range<N>>(ranges);

        // Condense adjacent ranges
        Collections.sort(values);
        Iterator<Range<N>> it = values.iterator();
        Range<N> r = it.next();
        while (it.hasNext()) {
            Range<N> r2 = it.next();
            Range<N> merged = r.merge(r2);
            if (merged != null) {
                r = merged;
            } else {
                buildNumericRangeFilter(filters, fieldName, r);
                r = r2;
            }
        }
        buildNumericRangeFilter(filters, fieldName, r);
    }

    private void buildPathFilters(List<Filter> filters, String fieldName, Set<String> values) throws ZepException {
        if (values == null || values.isEmpty()) return;
        final Analyzer analyzer = new LucenePathAnalyzer();
        for (String value : values) {
            final Filter filter;
            String value_without_leading_wildcard = StringUtils.trimLeadingCharacter(value, '*');
            if (value_without_leading_wildcard.startsWith("/")) {
                value = value_without_leading_wildcard.toLowerCase();
                // Starts-with
                if (value.endsWith("/")) {
                    filter = filterCache.get(PREFIX, new Term(nonAnalyzed(fieldName), value));
                }
                // Prefix
                else if (value.endsWith("*")) {
                    value = StringUtils.trimTrailingCharacter(value, '*');
                    filter = filterCache.get(PREFIX, new Term(nonAnalyzed(fieldName), value));
                }
                // Exact match
                else {
                    filter = filterCache.get(TERMS, new Term(nonAnalyzed(fieldName), value + "/"));
                }
            } else {
                final MultiPhraseQuery pq = new MultiPhraseQuery();
                filter = new QueryWrapperFilter(pq);

                //THIS GETTOKENS CALL IS STRIPPING THE WILDCARD
                List<String> tokens = getTokens(fieldName, analyzer, value);
                for (String token : tokens) {
                    Term[] terms = getMatchingTerms(fieldName, reader, token);
                    // Ensure we don't return results if term doesn't exist
                    if (terms.length == 0) {
                        pq.add(new Term(fieldName, token));
                    } else {
                        pq.add(terms);
                    }
                }
            }
            filters.add(filter);
        }
    }

    private void buildTermFilters(List<Filter> filters, String fieldName, Set<String> values) {
        if (values == null || values.isEmpty()) return;
        switch (occur) {
            case SHOULD:
                Term[] terms = new Term[values.size()];
                int i = 0;
                for (String value : values)
                    terms[i++] = new Term(fieldName, value);
                filters.add(filterCache.get(TERMS, terms));
                break;
            case MUST:
            case MUST_NOT:
                for (String value : values)
                    filters.add(filterCache.get(TERMS, new Term(fieldName, value)));
                break;
            default:
                throw new UnsupportedOperationException("Unexpected occur: " + occur);
        }
    }

    private void buildWildcardFilters(List<Filter> filters, String fieldName, Set<String> values) {
        if (values == null || values.isEmpty()) return;
        for (String value : values) {
            // Check for empty string
            final String tmp_value = StringUtils.trimTrailingCharacter(value,'*');
            final String unquoted = unquote(tmp_value);
            if (tmp_value.isEmpty() || unquoted.isEmpty())
                value = unquoted;
            Term term = new Term(fieldName, value);
            filters.add(filterCache.get(WILDCARD, term));
        }
    }

    /**
     * Tokenizes the given query using the same behavior as when the field is analyzed.
     *
     * @param fieldName The field name in the index.
     * @param analyzer  The analyzer to use to tokenize the query.
     * @param query     The query to tokenize.
     * @return The tokens from the query.
     * @throws ZepException If an exception occur.
     */
    private static List<String> getTokens(String fieldName, Analyzer analyzer, String query) throws ZepException {
        final List<String> tokens = new ArrayList<String>();
        try {
            TokenStream ts = analyzer.tokenStream(fieldName, new StringReader(query));
            CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
            try {
                ts.reset();
                while (ts.incrementToken()) {
                    tokens.add(term.toString());
                }
                ts.end();
            } catch (IOException e) {
                throw new ZepException(e.getLocalizedMessage(), e);
            } finally {
                ts.close();
            }
        } catch (IOException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        }
        return tokens;
    }

    private static Term[] getMatchingTerms(String fieldName, IndexReader reader, String value)
            throws ZepException {
        // Don't search for matches if text doesn't contain wildcards
        if (value.indexOf('*') == -1 && value.indexOf('?') == -1)
            return new Term[]{new Term(fieldName, value)};

        logger.debug("getMatchingTerms: field={}, value={}", fieldName, value);
        List<Term> matches = new ArrayList<Term>();
        Automaton automaton = WildcardQuery.toAutomaton(new Term(fieldName, value));
        CompiledAutomaton compiled = new CompiledAutomaton(automaton);
        try {
            Terms terms = SlowCompositeReaderWrapper.wrap(reader).terms(fieldName);
            TermsEnum wildcardTermEnum = compiled.getTermsEnum(terms);
            BytesRef match;
            while (wildcardTermEnum.next() != null) {
                match = wildcardTermEnum.term();
                logger.debug("Match: {}", match);
                matches.add(new Term(fieldName, match.utf8ToString()));
            }
            return matches.toArray(new Term[matches.size()]);
        } catch (IOException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        }
    }

    private static MultiPhraseQuery createIpAddressMultiPhraseQuery(String fieldName, IndexReader reader, String[] tokens)
            throws ZepException {
        final MultiPhraseQuery pq = new MultiPhraseQuery();
        for (String token : tokens) {
            token = removeLeadingZeros(token);
            Term[] terms = getMatchingTerms(fieldName, reader, token);
            if (terms.length == 0)
                pq.add(new Term(fieldName, token));
            else
                pq.add(terms);
        }
        return pq;
    }

}
