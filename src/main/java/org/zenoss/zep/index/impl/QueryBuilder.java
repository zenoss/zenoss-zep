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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.zenoss.protobufs.util.Util.TimestampRange;
import org.zenoss.protobufs.zep.Zep.FilterOperator;
import org.zenoss.protobufs.zep.Zep.NumberRange;
import org.zenoss.zep.ZepException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class QueryBuilder {
    private List<Query> queries = new ArrayList<Query>();

    public QueryBuilder addField(String key, String value) {
        queries.add(new TermQuery(new Term(key, value)));
        return this;
    }

    public QueryBuilder addWildcardFields(String key, Collection<String> values) {
        if (!values.isEmpty()) {
            final BooleanClause.Occur occur = BooleanClause.Occur.SHOULD;
            final BooleanQuery booleanQuery = new BooleanQuery();

            for (String value : values) {
                booleanQuery.add(new WildcardQuery(new Term(key, value.toLowerCase())), occur);
            }
            queries.add(booleanQuery);
        }
        return this;
    }

    public QueryBuilder addField(String key, List<String> values, FilterOperator op) throws ZepException {
        if (!values.isEmpty()) {
            final BooleanClause.Occur occur;
            final BooleanQuery booleanQuery = new BooleanQuery();
            if (op == FilterOperator.AND) {
                occur = BooleanClause.Occur.MUST;
            } else {
                occur = BooleanClause.Occur.SHOULD;
            }

            for (String value : values) {
                booleanQuery.add(new TermQuery(new Term(key, value)), occur);
            }
            queries.add(booleanQuery);
        }
        return this;
    }

    public QueryBuilder addFieldOfIntegers(String key, List<Integer> values) throws ZepException {
        if (!values.isEmpty()) {
            final BooleanClause.Occur occur = BooleanClause.Occur.SHOULD;
            final BooleanQuery booleanQuery = new BooleanQuery();

            // Condense adjacent values into one range
            Collections.sort(values);
            Iterator<Integer> it = values.iterator();
            int from = it.next();
            int to = from;
            while (it.hasNext()) {
                int value = it.next();
                if (value == to + 1) {
                    to = value;
                }
                else {
                    booleanQuery.add(NumericRangeQuery.newIntRange(key, from, to, true, true), occur);
                    from = to = value;
                }
            }
            booleanQuery.add(NumericRangeQuery.newIntRange(key, from, to, true, true), occur);

            queries.add(booleanQuery);
        }
        return this;
    }

    public QueryBuilder addFieldOfEnumNumbers(String key, List<? extends ProtocolMessageEnum> values) throws ZepException {
        List<Integer> valuesList = new ArrayList<Integer>(values.size());
        for ( ProtocolMessageEnum e : values ) {
            valuesList.add(Integer.valueOf(e.getNumber()));
        }
        addFieldOfIntegers(key, valuesList);
        return this;
    }

    public QueryBuilder addRange(String key, Long from, Long to) {
        this.queries.add(NumericRangeQuery.newLongRange(key, from, to, true, true));
        return this;
    }

    public QueryBuilder addTimestampRanges(String key, List<TimestampRange> ranges) {
        if (!ranges.isEmpty()) {
            final BooleanClause.Occur occur = BooleanClause.Occur.SHOULD;
            final BooleanQuery booleanQuery = new BooleanQuery();

            Long from = null, to = null;
            for (TimestampRange range : ranges) {
                if (range.hasStartTime()) {
                    from = range.getStartTime();
                }
                if (range.hasEndTime()) {
                    to = range.getEndTime();
                }
                booleanQuery.add(NumericRangeQuery.newLongRange(key, from, to, true, true), occur);
            }
            this.queries.add(booleanQuery);
        }
        return this;
    }

    public QueryBuilder addRanges(String key, Collection<NumberRange> ranges) throws ZepException {
        if (!ranges.isEmpty()) {
            final BooleanClause.Occur occur = BooleanClause.Occur.SHOULD;
            final BooleanQuery booleanQuery = new BooleanQuery();

            for (NumberRange range : ranges) {
                Integer from = null, to = null;
                if (range.hasFrom()) {
                    from = range.getFrom();
                }
                if (range.hasTo()) {
                    to = range.getTo();
                }
                booleanQuery.add(NumericRangeQuery.newIntRange(key, from, to, true, true), occur);
            }
            queries.add(booleanQuery);
        }
        return this;
    }

    public BooleanQuery build() {
        if (this.queries.isEmpty()) {
            return null;
        }
        
        BooleanQuery booleanQuery = new BooleanQuery();
        final BooleanClause.Occur occur = BooleanClause.Occur.MUST;
        for (Query query : this.queries) {
            booleanQuery.add(query, occur);
        }
        this.queries.clear();
        return booleanQuery;
    }

    @Override
    public String toString() {
        return "QueryBuilder{" +
                "queries=" + queries +
                '}';
    }
}
