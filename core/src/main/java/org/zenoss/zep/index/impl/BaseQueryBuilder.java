/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2010, 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.index.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ProtocolMessageEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.util.Util.TimestampRange;
import org.zenoss.protobufs.zep.Zep.EventDetailItem.EventDetailType;
import org.zenoss.protobufs.zep.Zep.EventDetailFilter;
import org.zenoss.protobufs.zep.Zep.NumberRange;
import org.zenoss.protobufs.zep.Zep.EventFilter;
import org.zenoss.protobufs.zep.Zep.EventTagFilter;
import org.zenoss.protobufs.zep.Zep.FilterOperator;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.utils.IpRange;
import org.zenoss.zep.utils.IpUtils;

import java.net.InetAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.zenoss.zep.index.impl.IndexConstants.*;

public abstract class BaseQueryBuilder<B extends BaseQueryBuilder<B>> {

    protected static final Logger logger = LoggerFactory.getLogger(BaseQueryBuilder.class);

    public static enum Occur {MUST, SHOULD, MUST_NOT}
    public static enum FieldType {
        DATE_RANGE,
        ENUM_NUMBER,
        FULL_TEXT,
        IDENTIFIER,
        IP_ADDRESS_SUBSTRING,
        IP_ADDRESS,
        IP_ADDRESS_RANGE,
        NUMERIC_RANGE,
        PATH,
        TERM,
        WILDCARD
    }
    protected final Occur occur;
    protected final Set<B> subClauses;
    private final Map<FieldType, Map<String, Set<?>>> fieldsValues;

    protected BaseQueryBuilder(Occur occur) {
        this.occur = occur;
        subClauses = Sets.newHashSet();
        fieldsValues = Maps.newEnumMap(FieldType.class);
        for (FieldType type : FieldType.values())
            fieldsValues.put(type, new HashMap<String, Set<?>>());
    }

    protected abstract B subBuilder(Occur occur);

    protected final synchronized <T> Set<T> getFieldSet(FieldType type, String fieldName) {
        Set<?> values = fieldsValues.get(type).get(fieldName);
        if (values == null) values = Sets.newHashSet();
        fieldsValues.get(type).put(fieldName, values);
        return (Set<T>) values;
    }

    protected final <T> void addFieldValues(FieldType type, String fieldName, Collection<T> values) {
        addFieldValues(type, fieldName, values, FilterOperator.OR);
    }

    protected final <T> void addFieldValues(FieldType type, String fieldName, Collection<T> values, FilterOperator op) {
        if (values == null || values.isEmpty()) return;
        if (op != FilterOperator.AND && op != FilterOperator.OR)
            throw new UnsupportedOperationException("Unexpected op: " + op);
        switch (occur) {
            case SHOULD:
                if (op == FilterOperator.AND) {
                    B sub = subBuilder(Occur.MUST);
                    sub.getFieldSet(type, fieldName).addAll(values);
                    subClauses.add(sub);
                } else
                    getFieldSet(type, fieldName).addAll(values);
                break;
            case MUST:
                if (op == FilterOperator.AND)
                    getFieldSet(type, fieldName).addAll(values);
                else {
                    B sub = subBuilder(Occur.SHOULD);
                    sub.getFieldSet(type, fieldName).addAll(values);
                    subClauses.add(sub);
                }
                break;
            case MUST_NOT:
                if (op == FilterOperator.AND) {
                    B sub = subBuilder(Occur.SHOULD);
                    sub.getFieldSet(type, fieldName).addAll(values);
                    subClauses.add(sub);
                } else
                    getFieldSet(type, fieldName).addAll(values);
                break;
            default:
                throw new UnsupportedOperationException("Unexpected occur: " + occur);
        }
    }

    protected final Set<Entry<String,Set<?>>> fieldsValues(FieldType type) {
        return fieldsValues.get(type).entrySet();
    }

    public final void addFilter(EventFilter filter) throws ZepException {
        addNumberRangeFields(FIELD_COUNT, filter.getCountRangeList());
        addWildcardFields(FIELD_CURRENT_USER_NAME, filter.getCurrentUserNameList());
        addIdentifierFields(FIELD_ELEMENT_IDENTIFIER, filter.getElementIdentifierList());
        addIdentifierFields(FIELD_ELEMENT_TITLE, filter.getElementTitleList());
        addIdentifierFields(FIELD_ELEMENT_SUB_IDENTIFIER, filter.getElementSubIdentifierList());
        addIdentifierFields(FIELD_ELEMENT_SUB_TITLE, filter.getElementSubTitleList());
        addWildcardFields(FIELD_FINGERPRINT, filter.getFingerprintList());
        addFullTextFields(FIELD_SUMMARY, filter.getEventSummaryList());
        addFullTextFields(FIELD_MESSAGE, filter.getMessageList());
        addTimestampRanges(FIELD_FIRST_SEEN_TIME, filter.getFirstSeenList());
        addTimestampRanges(FIELD_LAST_SEEN_TIME, filter.getLastSeenList());
        addTimestampRanges(FIELD_STATUS_CHANGE_TIME, filter.getStatusChangeList());
        addTimestampRanges(FIELD_UPDATE_TIME, filter.getUpdateTimeList());
        addFieldOfEnumNumbers(FIELD_STATUS, filter.getStatusList());
        addFieldOfEnumNumbers(FIELD_SEVERITY, filter.getSeverityList());
        addWildcardFields(FIELD_AGENT, filter.getAgentList());
        addWildcardFields(FIELD_MONITOR, filter.getMonitorList());
        addWildcardFields(FIELD_EVENT_KEY, filter.getEventKeyList());
        addWildcardFields(FIELD_EVENT_CLASS_KEY, filter.getEventClassKeyList());
        addWildcardFields(FIELD_EVENT_GROUP, filter.getEventGroupList());
        addPathFields(FIELD_EVENT_CLASS, filter.getEventClassList());

        for (EventTagFilter tagFilter : filter.getTagFilterList()) {
            addTermFields(FIELD_TAGS, tagFilter.getTagUuidsList(), tagFilter.getOp());
        }
        addWildcardFields(FIELD_UUID, filter.getUuidList());

        addDetails(filter.getDetailsList());

        for (EventFilter subFilter : filter.getSubfilterList()) {
            Occur subOccur = subFilter.getOperator() == FilterOperator.OR ? Occur.SHOULD : Occur.MUST;
            B sub = subBuilder(subOccur);
            sub.addFilter(subFilter);
            subClauses.add(sub);
        }
    }

    protected final void addTermField(String fieldName, String value) {
        getFieldSet(FieldType.TERM, fieldName).add(value);
    }

    protected final void addTermFields(String fieldName, Collection<String> values, FilterOperator op) {
        addFieldValues(FieldType.TERM, fieldName, values, op);
    }

    /**
     * Special case queries for event classes. Queries that begin with a slash and end
     * with a slash or an asterisk result in a starts-with query on the non-analyzed
     * field name. Queries that begin with a slash and end with anything else are an
     * exact match on the non-analyzed field name. Otherwise, the query is run through
     * the analyzer and substring matching is performed.
     *
     * @param fieldName    Analyzed field name.
     * @param paths        Queries to search on.
     */
    protected final void addPathFields(String fieldName, Collection<String> paths) {
        addFieldValues(FieldType.PATH, fieldName, paths);
    }

    protected final void addFieldOfEnumNumbers(String fieldName, Collection<? extends ProtocolMessageEnum> numbers) {
        addFieldValues(FieldType.ENUM_NUMBER, fieldName, numbers);
    }

    protected final void addFullTextFields(String fieldName, List<String> values) {
        addFieldValues(FieldType.FULL_TEXT, fieldName, values);
    }

    public void addRange(String fieldName, Long from, Long to) {
        getFieldSet(FieldType.NUMERIC_RANGE, fieldName).add(new Range<Long>(from, to));
    }

    protected final void addTimestampRanges(String fieldName, Collection<TimestampRange> ranges) {
        addFieldValues(FieldType.DATE_RANGE, fieldName, ranges);
    }

    protected final void addNumberRangeFields(String fieldName, Collection<NumberRange> ranges) {
        final List<Range<Long>> countRangeList = Lists.newArrayList();
        for (NumberRange range : ranges) {
            countRangeList.add(new Range<Long>(range.hasFrom() ? range.getFrom() : null,
                    range.hasTo() ? range.getTo() : null));
        }
        addFieldValues(FieldType.NUMERIC_RANGE, fieldName, countRangeList);
    }

    protected final <T extends Number & Comparable<T>> void addNumericRangeField(String fieldName, Range<T> range) {
        getFieldSet(FieldType.NUMERIC_RANGE, fieldName).add(range);
    }

    /**
     * Special case queries for identifier fields. Queries that are enclosed in quotes result in
     * an exact query for the string in the non-analyzed field. Queries that end in an asterisk
     * result in a prefix query in the non-analyzed field. Queries of a length less than
     * the {@link IndexConstants#MIN_NGRAM_SIZE} are converted to prefix queries on the
     * non-analyzed field. All other queries are sent to the NGram analyzed field for efficient
     * substring matches.
     *
     * @param fieldName     Analyzed field name.
     * @param values        Queries to search on.
     */
    protected final void addIdentifierFields(String fieldName, Collection<String> values) {
        addFieldValues(FieldType.IDENTIFIER, fieldName, values);
    }

    protected final void addIpAddressSubstringField(String fieldName, String value) {
        getFieldSet(FieldType.IP_ADDRESS_SUBSTRING, fieldName).add(value);
    }

    protected final void addIpAddressField(String fieldName, InetAddress value) {
        getFieldSet(FieldType.IP_ADDRESS, fieldName).add(value);
    }

    protected final void addIpAddressRangeField(String fieldName, IpRange value) {
        getFieldSet(FieldType.IP_ADDRESS_RANGE, fieldName).add(value);
    }

    protected final void addWildcardFields(String fieldName, Collection<String> values) {
        addFieldValues(FieldType.WILDCARD, fieldName, values);
    }

    private interface StringToNumber<T extends Number & Comparable<T>> {
        T convert(String s);
    }

    private final StringToNumber<Integer> INTEGER_CONVERTER = new StringToNumber<Integer>() {
        @Override
        public Integer convert(String s) {
            return (s == null || s.isEmpty()) ? null : Integer.valueOf(s);
        }
    };

    private final StringToNumber<Float> FLOAT_CONVERTER = new StringToNumber<Float>() {
        @Override
        public Float convert(String s) {
            return (s == null || s.isEmpty()) ? null : Float.valueOf(s);
        }
    };

    private final StringToNumber<Long> LONG_CONVERTER = new StringToNumber<Long>() {
        @Override
        public Long convert(String s) {
            return (s == null || s.isEmpty()) ? null : Long.valueOf(s);
        }
    };

    private final StringToNumber<Double> DOUBLE_CONVERTER = new StringToNumber<Double>() {
        @Override
        public Double convert(String s) {
            return (s == null || s.isEmpty()) ? null : Double.valueOf(s);
        }
    };

    public static class Range<T extends Comparable<T>> implements Comparable<Range<T>> {
        public final T from, to;
        public Range(T from, T to) {
            if (from != null && to != null && from.compareTo(to) > 0)
                throw new IllegalArgumentException("inverted range");
            this.from = from;
            this.to = to;
        }
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append('(');
            sb.append(from);
            sb.append('-');
            sb.append(to);
            sb.append(')');
            return sb.toString();
        }

        @Override
        public int hashCode() {
            int result = from != null ? from.hashCode() : 0;
            result = 31 * result + (to != null ? to.hashCode() : 0);
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof Range) {
                Range that = (Range)o;
                return ((this.from == null) ? (that.from == null) : this.from.equals(that.from)) &&
                       ((this.to == null) ? (that.to == null) : this.to.equals(that.to));
            }
            return false;
        }

        @Override
        public int compareTo(Range<T> that) {
            if (this.from == null) {
                if (that.from == null) {
                    if (this.to == null) {
                        if (that.to == null)
                            return 0;
                        else
                            return 1;
                    } else {
                        if (that.to == null)
                            return -1;
                        else
                            return this.to.compareTo(that.to);
                    }
                } else {
                    return -1;
                }
            } else {
                if (that.from == null)
                    return 1;
                else {
                    int i = this.from.compareTo(that.from);
                    if (i != 0)
                        return i;
                    if (this.to == null) {
                        if (that.to == null)
                            return 0;
                        else
                            return 1;
                    } else {
                        if (that.to == null)
                            return -1;
                        else
                            return this.to.compareTo(that.to);
                    }
                }
            }
        }

        /** @return null if ranges cannot be merged (because they don't overlap and are not adjacent) */
        public Range<T> merge(Range<T> that) {
            if (this.to != null && that.from != null && this.to.compareTo(that.from) < 0 ) {
                Object o = this.to;
                if ((o instanceof Long) || (o instanceof Integer)) {
                    if (((Number)this.to).longValue() + 1 == ((Number)that.from).longValue()) {
                        return new Range<T>(this.from, that.to);
                    }
                }
                return null;
            } else if (that.to != null && this.from != null && that.to.compareTo(this.from) < 0) {
                Object o = this.to;
                if ((o instanceof Long) || (o instanceof Integer)) {
                    if (((Number)that.to).longValue() + 1 == ((Number)this.from).longValue()) {
                        return new Range<T>(that.from, this.to);
                    }
                }
                return null;
            }
            return new Range<T>(this.from == null || that.from == null ? null : this.from.compareTo(that.from) <= 0 ? this.from : that.from,
                                this.to == null || that.to == null ? null : this.to.compareTo(that.to) >= 0 ? this.to : that.to);
        }
    }

    private static <T extends Number & Comparable<T>>  Range<T> parseNumericValue(String value, StringToNumber<T> converter) throws ZepException {
        if (value.isEmpty()) {
            throw new ZepException("Empty numeric value");
        }
        final String left, right;
        int colonIndex = value.indexOf(':');

        // any ':' means this is a range of some kind.
        if (colonIndex == -1) {
            left = right = value;
        } else {
            left = value.substring(0, colonIndex);
            right = value.substring(colonIndex + 1);
        }
        return new Range<T>(converter.convert(left), converter.convert(right));
    }

    protected static String nonAnalyzed(String fieldName) {
        String result = NON_ANALYZED.get(fieldName);
        if (result != null)
            return result;
        return fieldName + SORT_SUFFIX;
    }

    protected static String unquote(String str) {
        final int len = str.length();
        String unquoted = str;
        if (len >= 2 && str.charAt(0) == '"' && str.charAt(len - 1) == '"') {
            unquoted = str.substring(1, len - 1);
        }
        return unquoted;
    }

    protected static final Pattern LEADING_ZEROS = Pattern.compile("0+(.*)");

    protected static String removeLeadingZeros(String original) {
        String ret = original;
        final Matcher matcher = LEADING_ZEROS.matcher(original);
        if (matcher.matches()) {
            final String remaining = matcher.group(1);
            if (remaining.isEmpty()) {
                ret = "0";
            } else {
                // Preserve leading zero if next character is non numeric
                final char firstChar = Character.toLowerCase(remaining.charAt(0));
                if ((firstChar < '0' || firstChar > '9') && (firstChar < 'a' || firstChar > 'f')) {
                    ret = "0" + remaining;
                } else {
                    ret = remaining;
                }
            }
        }
        return ret;
    }


    private void addDetails(Collection<EventDetailFilter> filters)
            throws ZepException {
        if (!filters.isEmpty()) {
            for (EventDetailFilter edf : filters) {
                B sub = subBuilder(edf.getOp().equals(FilterOperator.OR) ? Occur.SHOULD : Occur.MUST);
                final String key = getDetailKey(edf.getKey());
                
                if (key == null) {
                    throw new ZepException("Event detail is not indexed: " + edf.getKey());
                }
                EventDetailType detailType = getDetailType(edf.getKey());
                switch (detailType) {
                    case STRING:
                        sub.addWildcardFields(key, edf.getValueList());
                        break;
                    case INTEGER:
                        for (String val : edf.getValueList())
                            sub.addNumericRangeField(key, parseNumericValue(val, INTEGER_CONVERTER));
                        break;
                    case FLOAT:
                        for (String val : edf.getValueList())
                            sub.addNumericRangeField(key, parseNumericValue(val, FLOAT_CONVERTER));
                        break;
                    case LONG:
                        for (String val : edf.getValueList())
                            sub.addNumericRangeField(key, parseNumericValue(val, LONG_CONVERTER));
                        break;
                    case DOUBLE:
                        for (String val : edf.getValueList())
                            sub.addNumericRangeField(key, parseNumericValue(val, DOUBLE_CONVERTER));
                        break;
                    case PATH:
                        sub.addPathFields(key, edf.getValueList());
                        break;
                    case IP_ADDRESS:
                        for (String val : edf.getValueList()) {
                            val = unquote(val);
                            try {
                                // Try to parse as IP range
                                IpRange range = IpUtils.parseRange(val);
                                if (range.getFrom().equals(range.getTo()))
                                    sub.addIpAddressField(key, range.getFrom());
                                else
                                    sub.addIpAddressRangeField(key, range);
                            } catch (IllegalArgumentException e) {
                                // Didn't match IP range - try performing a substring match
                                sub.addIpAddressSubstringField(key, val);
                            }
                        }
                        break;
                    default:
                        throw new ZepException("Unsupported detail type: " + detailType);
                }

                if (!sub.isEmpty()) {
                    this.subClauses.add(sub);
                }
            }
        }
    }

    protected abstract EventDetailType getDetailType(String key) throws ZepException;

    protected abstract String getDetailKey(String key) throws ZepException;

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("[");
        sb.append(occur);
        sb.append(" match : ");
        boolean any = false;
        for (FieldType ft : FieldType.values()) {
            Map<String, Set<?>> fieldValues = fieldsValues.get(ft);
            if (fieldValues == null || fieldValues.isEmpty()) continue;
            if (any) sb.append(", ");
            sb.append(ft);
            sb.append(": ");
            sb.append(fieldValues);
            any = true;
        }
        if (!subClauses.isEmpty()) {
            if (any) sb.append(", ");
            sb.append("SUB_CLAUSES: ");
            sb.append(subClauses);
            any = true;
        }
        sb.append("]");
        return sb.toString();
    }

    protected boolean isEmpty() {
        for (Map<String, Set<?>> fieldValues : fieldsValues.values())
            if (!fieldValues.isEmpty()) return false;
        for (B sub : subClauses) {
            if (!sub.isEmpty()) return false;
        }
        return true;
    }
}
