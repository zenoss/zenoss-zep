/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2010, Zenoss Inc.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 *
 * For complete information please visit: http://www.zenoss.com/oss/
 */

package org.zenoss.zep.index.impl;

import com.google.protobuf.ProtocolMessageEnum;
import org.apache.solr.schema.DateField;
import org.zenoss.protobufs.zep.Zep.FilterOperator;
import org.zenoss.protobufs.zep.Zep.NumberCondition;
import org.zenoss.zep.ZepException;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SolrQueryBuilder {
    private Map<String, Object> fields = new HashMap<String, Object>();

    private Pattern escapePattern = Pattern.compile("(?<!\\\\)([&|+\\-!(){}\\[\\]^\"~*?:])");

    // Escape all the disallowed characters except "*" for wildcard searches
    private Pattern wildcardEscapePattern = Pattern.compile("(?<!\\\\)([&|+\\-!(){}\\[\\]^\"~?:])");

    private final DateField dateField = new DateField();

    private String quote(String value) {
        if ( value.contains(" ") ) {
            value = String.format("\"%s\"", value);
        }

        return value;
    }
    
    private String escape(String value) {
        Matcher escapeMatcher = escapePattern.matcher(value);
        return quote(escapeMatcher.replaceAll("\\\\$1"));
    }

    private String escapeWithWildcard(String value) {
        Matcher wildcardEscapeMatcher = wildcardEscapePattern.matcher(value);
        return quote(wildcardEscapeMatcher.replaceAll("\\\\$1"));
    }

    public Map<String, Object> getParams() {
        return fields;
    }

    public void addField(String key, String value) {
        fields.put(key, escape(value));
    }

    public void addWildcardField(String key, String value) {
        fields.put(key, escapeWithWildcard(value));
    }

    public void addField(String key, int value) {
        fields.put(key, String.valueOf(value));
    }

    public void addField(String key, long value) {
        fields.put(key, String.valueOf(value));
    }

    public void addField(String key, Date value) {
        fields.put(key, dateAsString(value));
    }

    public void addField(String key,  NumberCondition condition) throws ZepException {
        int value = condition.getValue();

        final String valueStr;
        if ( condition.getOp() == NumberCondition.Operation.GT ) {
            if ( value == Integer.MAX_VALUE ) {
                throw new ZepException("Condition " + key + " has too large a value");
            }

            valueStr = String.format("{%s TO *}", value);
        }
        else if ( condition.getOp() == NumberCondition.Operation.GTEQ ) {
            valueStr = String.format("[%s TO *]", value);
        }
        else if ( condition.getOp() == NumberCondition.Operation.LT ) {
            if ( value == Integer.MIN_VALUE ) {
                throw new ZepException("Condition " + key + " has too small a value");
            }

            valueStr = String.format("{* TO %s}", value);
        }
        else if ( condition.getOp() == NumberCondition.Operation.LTEQ ) {
            valueStr = String.format("[* TO %s]", value);
        }
        else {
            valueStr = Integer.toString(value);
        }

        fields.put(key, valueStr);
    }

    public void addField(String key, List<String> values, FilterOperator op) throws ZepException {
        if ( values.size() == 0 ) {
            throw new ZepException("You can not search on an empty list.");
        }

        StringBuilder sb = new StringBuilder();

        Iterator<String> it = values.iterator();
        while (it.hasNext()) {
            sb.append(String.format("\"%s\" ", escape(it.next())));
            if (it.hasNext()) {
                sb.append(' ').append(op.name()).append(' ');
            }
        }

        fields.put(key, "(" + sb.toString() + ")");
    }

    public void addFieldOfIntegers(String key, List<Integer> values) throws ZepException {
        if ( values.size() == 0 ) {
            throw new ZepException("You can not search on an empty list.");
        }

        StringBuilder sb = new StringBuilder();

        Iterator<Integer> it = values.iterator();
        while (it.hasNext()) {
            sb.append(it.next());
            if (it.hasNext()) {
                sb.append(" OR ");
            }
        }

        fields.put(key, "(" + sb.toString() + ")");
    }

    public void addFieldOfEnumNumbers(String key, List<? extends ProtocolMessageEnum> values) throws ZepException {
        List<Integer> valuesList = new ArrayList<Integer>(values.size());
        for ( ProtocolMessageEnum e : values ) {
            valuesList.add(Integer.valueOf(e.getNumber()));
        }
        addFieldOfIntegers(key, valuesList);
    }

    public void addFieldOfEnumNames(String key, List<? extends Enum<?>> values) throws ZepException {
        List<String> strValues = new ArrayList<String>(values.size());
        for ( Enum<?> e : values ) {
            strValues.add(e.name());
        }
        addField(key, strValues, FilterOperator.OR);
    }

    public void addRange(String key, int from, int to) {
        fields.put(key, String.format("[%d TO %d]", from, to));
    }

    public void addRange(String key, long from, long to) {
        fields.put(key, String.format("[%d TO %d]", from, to));
    }

    public void addRange(String key, Date from, Date to) {
        fields.put(key, String.format("[%s TO %s]", dateAsString(from), dateAsString(to)));
    }

    private String dateAsString(Date date) {
        String str = "*";
        if ( date != null ) {
            str = dateField.toExternal(date);
        }

        return str;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        Iterator<Map.Entry<String, Object>> it = fields.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String,Object> pairs = it.next();
            sb.append(pairs.getKey()).append(':').append(pairs.getValue());
            if (it.hasNext()) {
                sb.append(" AND ");
            }
        }
        return sb.toString();
    }
}
