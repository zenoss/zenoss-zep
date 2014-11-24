/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.index.impl.solr;

import com.google.common.collect.*;
import com.google.protobuf.ProtocolMessageEnum;
import org.springframework.util.StringUtils;
import org.zenoss.protobufs.util.Util.TimestampRange;
import org.zenoss.protobufs.zep.Zep.EventDetailItem;
import org.zenoss.protobufs.zep.Zep.EventDetailItem.EventDetailType;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.index.IndexedDetailsConfiguration;
import org.zenoss.zep.index.impl.BaseQueryBuilder;
import org.zenoss.zep.index.impl.IndexConstants;
import org.zenoss.zep.utils.IpRange;
import org.zenoss.zep.utils.IpUtils;

import java.net.InetAddress;
import java.util.*;
import java.util.Map.Entry;

public class SolrQueryBuilder extends BaseQueryBuilder<SolrQueryBuilder> {

    private final IndexedDetailsConfiguration indexedDetailsConfiguration;
    private transient List<String> clauses;

    public SolrQueryBuilder(IndexedDetailsConfiguration configuration) {
        this(Occur.MUST, configuration);
    }

    public SolrQueryBuilder(Occur occur, IndexedDetailsConfiguration configuration) {
        super(occur);
        this.indexedDetailsConfiguration = configuration;
    }

    @Override
    protected SolrQueryBuilder subBuilder(Occur occur) {
        return new SolrQueryBuilder(occur, indexedDetailsConfiguration);
    }

    @Override
    protected EventDetailType getDetailType(String key) throws ZepException {
        EventDetailItem item = indexedDetailsConfiguration.getEventDetailItemsByName().get(key);
        return (item == null) ? null : item.getType();
    }

    @Override
    protected String getDetailKey(String key) throws ZepException {
        EventDetailItem item = indexedDetailsConfiguration.getEventDetailItemsByName().get(key);
        if (item == null) return null;
        switch (item.getType()) {
            case STRING:
                return SolrEventIndexMapper.DETAIL_INDEX_PREFIX + item.getKey() + "_s";
            case INTEGER:
                return SolrEventIndexMapper.DETAIL_INDEX_PREFIX + item.getKey() + "_i";
            case FLOAT:
                return SolrEventIndexMapper.DETAIL_INDEX_PREFIX + item.getKey() + "_f";
            case LONG:
                return SolrEventIndexMapper.DETAIL_INDEX_PREFIX + item.getKey() + "_l";
            case DOUBLE:
                return SolrEventIndexMapper.DETAIL_INDEX_PREFIX + item.getKey() + "_d";
            case IP_ADDRESS:
                return SolrEventIndexMapper.DETAIL_INDEX_PREFIX + item.getKey() + "_ip";
            case PATH:
                return SolrEventIndexMapper.DETAIL_INDEX_PREFIX + item.getKey() + "_path";
            default:
                throw new IllegalStateException("Unexpected type: " + item.getType());
        }
    }

    public synchronized String build() {
        this.clauses = Lists.newArrayList();

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.DATE_RANGE))
            useDateRange(field.getKey(), (Set<TimestampRange>) field.getValue());

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.ENUM_NUMBER))
            useEnumNumber(field.getKey(), (Set<ProtocolMessageEnum>) field.getValue());

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.FULL_TEXT))
            useFullText(field.getKey(), (Set<String>) field.getValue());

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.IDENTIFIER))
            useIdentifier(field.getKey(), (Set<String>) field.getValue());

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.IP_ADDRESS_SUBSTRING))
            useIpAddressSubstring(field.getKey(), (Set<String>) field.getValue());

        for (Entry<String, Set<?>> field: fieldsValues(FieldType.IP_ADDRESS))
            useIpAddress(field.getKey(), (Set<InetAddress>) field.getValue());

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.IP_ADDRESS_RANGE))
            useIpAddressRange(field.getKey(), (Set<IpRange>) field.getValue());

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.NUMERIC_RANGE))
            // Might not be Integers, but Java does type-erasure, so who cares.
            useNumericRange(field.getKey(), (Set<Range<Integer>>) field.getValue());

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.PATH))
            usePath(field.getKey(), (Set<String>) field.getValue());

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.TERM))
            useTerm(field.getKey(), (Set<String>) field.getValue());

        for (Entry<String, Set<?>> field : fieldsValues(FieldType.WILDCARD))
            useWildcard(field.getKey(), (Set<String>) field.getValue());

        for (SolrQueryBuilder sub : subClauses) {
            String clause = sub.build();
            if (clause != null)
                clauses.add(clause);
        }

        if (clauses.isEmpty()) return null;
        final String junction = (occur == Occur.SHOULD) ? " OR " : " AND ";
        final StringBuilder sb = new StringBuilder();
        int i = 0;
        for (String clause : clauses) {
            if (i++ > 0)
                sb.append(junction);
            if (occur == Occur.MUST_NOT)
                sb.append("NOT (");
            else
                sb.append("(");
            sb.append(clause);
            sb.append(")");
        }
        this.clauses = null;
        return sb.toString();
    }

    private void useDateRange(String fieldName, Set<TimestampRange> ranges) {
        if (ranges == null || ranges.isEmpty()) return;
        for (TimestampRange range : ranges) {
            if (range.hasStartTime() || range.hasEndTime()) {
                clauses.add(rangeQuery(fieldName,
                        range.hasStartTime() ? range.getStartTime() : null,
                        range.hasEndTime() ? range.getEndTime() : null));
            }
        }
    }

    private void useEnumNumber(String fieldName, Set<ProtocolMessageEnum> enums) {
        if (enums == null || enums.isEmpty()) return;
        List<Integer> values = new ArrayList<Integer>(enums.size());
        for (ProtocolMessageEnum e : enums)
            values.add(e.getNumber());

        // Condense adjacent values into one range
        Collections.sort(values);
        Iterator<Integer> it = values.iterator();
        Integer from = it.next();
        Integer to = from;
        while (it.hasNext()) {
            int value = it.next();
            if (value == to + 1) {
                to = value;
            } else {
                clauses.add(rangeQuery(fieldName, from, to));
                from = to = value;
            }
        }
        clauses.add(rangeQuery(fieldName, from, to));
    }

    private void useFullText(String fieldName, Set<String> values) {
        if (values == null || values.isEmpty()) return;
        for (String value: values) {
            // Check for empty string
            final String tmp_value = StringUtils.trimLeadingCharacter(StringUtils.trimTrailingCharacter(value,'*'),'*');
            if (tmp_value.isEmpty())
                continue;
            else {
                final String unquoted = unquote(tmp_value);
                if (unquoted.isEmpty())
                    continue;
            }
            clauses.add(complexPhraseQuery(fieldName, StringUtils.trimLeadingCharacter(value,'*')));
        }
    }

    private void useIdentifier(String fieldName, Set<String> values) {
        if (values == null || values.isEmpty()) return;
        for (String value : values) {
            value = StringUtils.trimTrailingCharacter(value, '*');
            final String unquoted = unquote(value);
            if (value.isEmpty() || !unquoted.equals(value)) {
                clauses.add(termQuery(nonAnalyzed(fieldName), unquoted));
            } else if (value.length() < IndexConstants.MIN_NGRAM_SIZE) {
                clauses.add(prefixQuery(fieldName, value));
            } else {
                clauses.add(termQuery(fieldName, value));
            }
        }
    }

    private void useIpAddressSubstring(String fieldName, Set<String> values) {
        if (values == null || values.isEmpty()) return;
        for (String value : values) {
            if (value.indexOf('.') >= 0) {
                StringBuilder sb = new StringBuilder();
                sb.append("(+");
                sb.append(escape(fieldName + IndexConstants.IP_ADDRESS_TYPE_SUFFIX));
                sb.append(':');
                sb.append(escape(IndexConstants.IP_ADDRESS_TYPE_4));
                sb.append(" +");
                String[] tokens = StringUtils.tokenizeToStringArray(value, ".");
                if (tokens.length > 0) {
                    sb.append(ipAddressComplexPhraseQuery(fieldName, tokens));
                } else {
                    sb.append(termQuery(fieldName, value));
                }
                sb.append(")");
                clauses.add(sb.toString());
            } else if (value.indexOf(':') >= 0) {
                StringBuilder sb = new StringBuilder();
                sb.append("(+");
                sb.append(escape(fieldName + IndexConstants.IP_ADDRESS_TYPE_SUFFIX));
                sb.append(':');
                sb.append(escape(IndexConstants.IP_ADDRESS_TYPE_6));
                sb.append(" +");
                String[] tokens = StringUtils.tokenizeToStringArray(value, ":");
                if (tokens.length > 0) {
                    sb.append(ipAddressComplexPhraseQuery(fieldName, tokens));
                } else {
                    sb.append(termQuery(fieldName, value));
                }
                sb.append(")");
                clauses.add(sb.toString());
            } else {
                useWildcard(fieldName, Collections.singleton(removeLeadingZeros(value)));
            }
        }
    }

    private void useIpAddress(String fieldName, Set<InetAddress> values) {
        if (values == null || values.isEmpty()) return;
        for (InetAddress addr : values) {
            clauses.add(termQuery(fieldName + "_sort", IpUtils.canonicalIpAddress(addr)));
        }
    }

    private void useIpAddressRange(String fieldName, Set<IpRange> values) {
        if (values == null || values.isEmpty()) return;
        for (IpRange range : values) {
            if (range.getFrom().equals(range.getTo()))
                clauses.add(termQuery(fieldName + "_sort", IpUtils.canonicalIpAddress(range.getFrom())));
            else
                clauses.add(rangeQuery(fieldName + "_sort", IpUtils.canonicalIpAddress(range.getFrom()), IpUtils.canonicalIpAddress(range.getTo())));
        }
    }

    private <N extends Number & Comparable<N>> void useNumericRange(String fieldName, Set<Range<N>> ranges) {
        if (ranges == null || ranges.isEmpty()) return;

        if (occur == Occur.MUST) {
            for (Range<N> range : ranges)
                if (range.from != null || range.to != null)
                    clauses.add(rangeQuery(fieldName, range.from, range.to));
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
                clauses.add(rangeQuery(fieldName, r.from, r.to));
                r = r2;
            }
        }
        clauses.add(rangeQuery(fieldName, r.from, r.to));
    }

    private void usePath(String fieldName, Set<String> values) {
        if (values == null || values.isEmpty()) return;
        for (String value : values) {
            if (value.startsWith("/")) {
                if (value.endsWith("/"))
                    clauses.add(prefixQuery(nonAnalyzed(fieldName), value));
                else if (value.endsWith("*"))
                    clauses.add(prefixQuery(nonAnalyzed(fieldName), StringUtils.trimTrailingCharacter(value, '*')));
                else
                    clauses.add(termQuery(nonAnalyzed(fieldName), value));
            } else {
                clauses.add(complexPhraseQuery(fieldName, StringUtils.trimLeadingCharacter(StringUtils.trimTrailingCharacter(value,'*'),'*')));
            }
        }
    }

    private void useTerm(String fieldName, Set<String> values) {
        if (values == null || values.isEmpty()) return;
        switch (occur) {
            case SHOULD:
                clauses.add(termsQuery(fieldName, values));
                break;
            case MUST:
            case MUST_NOT:
                for (String value : values)
                    clauses.add(termsQuery(fieldName, Collections.singleton(value)));
                break;
            default:
                throw new UnsupportedOperationException("Unexpected occur: " + occur);
        }
    }

    private void useWildcard(String fieldName, Set<String> values) {
        if (values == null || values.isEmpty()) return;
        for (String value : values) {
            // Check for empty string
            final String tmp_value = StringUtils.trimLeadingCharacter(StringUtils.trimTrailingCharacter(value,'*'),'*');
            if (tmp_value.isEmpty())
                continue;
            else {
                final String unquoted = unquote(tmp_value);
                if (unquoted.isEmpty())
                    continue;
            }
            clauses.add(escape(fieldName) + ":" + escape(value, USUAL_EXCLUDING_WILDCARDS, true));
        }
    }

    private static String join(String delimiter, Collection<String> values) {
        if (values == null || values.isEmpty()) return null;

        int length = delimiter.length() * (values.size() - 1);
        for (String value : values)
            length += value.length();

        StringBuilder sb = new StringBuilder(length);
        int i = 0;
        for (String value : values) {
            if (i++ > 0) sb.append(delimiter);
            sb.append(value);
        }
        return sb.toString();
    }

    private static final char[] USUAL                     = "+-&|!(){}[]^\"~:\\/*?".toCharArray();
    private static final char[] ESCAPE_FOR_TERMS_QUERY    = "+&|!(){}[]^\"~:\\/*?,".toCharArray();
    private static final char[] USUAL_EXCLUDING_WILDCARDS = "+-&|!(){}[]^\"~:\\/".toCharArray();
    static {
        Arrays.sort(USUAL);
        Arrays.sort(ESCAPE_FOR_TERMS_QUERY);
        Arrays.sort(USUAL_EXCLUDING_WILDCARDS);
    }

    private static String escape(String s) {
        return escape(s, USUAL, true);
    }

    private static String escapeButDontQuote(String s) {
        return escape(s, USUAL, false);
    }

    private static String escape(String s, char[] escaped, boolean quote) {
        StringBuilder sb = new StringBuilder();
        final int len = s.length();
        boolean whitespace = false;
        for (int i=0; i<len; i++) {
            final char c = s.charAt(i);
            if (Character.isWhitespace(c))
                whitespace = true;
            if (Arrays.binarySearch(escaped, c) >= 0)
                sb.append('\\');
            sb.append(c);
        }
        if (quote && (whitespace || sb.length() == 0))
            return '"' + sb.append('"').toString();
        else
            return sb.toString();
    }

    private static String rangeQuery(final String fieldName, final Object from, final Object to) {
        final StringBuilder sb = new StringBuilder();
        sb.append(escape(fieldName));
        sb.append(":[");
        sb.append(from == null ? "*" : escape(from.toString()));
        sb.append(" TO ");
        sb.append(to == null ? "*" : escape(to.toString()));
        sb.append("]");
        return sb.toString();
    }

    private static String localParamQuery(final String query, final String... params) {
        StringBuilder sb = new StringBuilder("_query_:\"{!");
        int i = 0;
        if (params.length % 2 == 1)
            sb.append(params[i++]);
        for (;i<params.length;) {
            if (i > 0) sb.append(" ");
            sb.append(escape(params[i++]));
            sb.append('=');
            sb.append(escape(params[i++]));
        }
        sb.append('}');
        if (query != null) sb.append(escapeButDontQuote(query));
        sb.append('"');
        return sb.toString();
    }

    private static String complexPhraseQuery(final String fieldName, final String value) {
        return localParamQuery(escape(fieldName) + ':' + escape(value, USUAL_EXCLUDING_WILDCARDS, true), "complexphrase");
    }

    private static String prefixQuery(final String fieldName, final String value) {
        return unquote(fieldName) + ':' + escape(value) + '*';
    }

    private static String termQuery(final String fieldName, final String value) {
        return unquote(fieldName) + ':' + escape(value);
    }

    private static String termsQuery(final String fieldName, final Collection<String> terms) {
        List<String> escapedTerms = new ArrayList<String>(terms.size());
        for (String term : terms)
            escapedTerms.add(escape(term, ESCAPE_FOR_TERMS_QUERY, true));
        return localParamQuery(join(",",escapedTerms), "terms", "f", escape(fieldName));
    }

    private static String ipAddressComplexPhraseQuery(String fieldName, String[] tokens) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (String token : tokens) {
            if (i++ > 0) sb.append(" ");
            sb.append(removeLeadingZeros(token));
        }
        return complexPhraseQuery(fieldName, sb.toString());
    }
}
