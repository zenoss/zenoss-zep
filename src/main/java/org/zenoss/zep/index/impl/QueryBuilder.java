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

import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ProtocolMessageEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.zenoss.protobufs.zep.Zep.FilterOperator;
import org.zenoss.protobufs.zep.Zep.NumberCondition;
import org.zenoss.zep.ZepException;

public class QueryBuilder {
    private List<Query> queries = new ArrayList<Query>();

    public QueryBuilder addField(String key, String value) {
        queries.add(new TermQuery(new Term(key, value)));
        return this;
    }

    public QueryBuilder addWildcardField(String key, String value) {
        queries.add(new WildcardQuery(new Term(key, value)));
        return this;
    }

    public QueryBuilder addField(String key, int value) {
        queries.add(NumericRangeQuery.newIntRange(key, value, value, true, true));
        return this;
    }

    public QueryBuilder addField(String key, long value) {
        queries.add(NumericRangeQuery.newLongRange(key, value, value, true, true));
        return this;
    }

    public QueryBuilder addField(String key,  NumberCondition condition) throws ZepException {
        final NumericRangeQuery query;
        int value = condition.getValue();

        final String valueStr;
        if ( condition.getOp() == NumberCondition.Operation.GT ) {
            if ( value == Integer.MAX_VALUE ) {
                throw new ZepException("Condition " + key + " has too large a value");
            }
            query = NumericRangeQuery.newIntRange(key, value, null, false, true);
        }
        else if ( condition.getOp() == NumberCondition.Operation.GTEQ ) {
            query = NumericRangeQuery.newIntRange(key, value, null, true, true);
        }
        else if ( condition.getOp() == NumberCondition.Operation.LT ) {
            if ( value == Integer.MIN_VALUE ) {
                throw new ZepException("Condition " + key + " has too small a value");
            }
            query = NumericRangeQuery.newIntRange(key, null, value, true, false);
        }
        else if ( condition.getOp() == NumberCondition.Operation.LTEQ ) {
            query = NumericRangeQuery.newIntRange(key, null, value, true, true);
        }
        else {
            query = NumericRangeQuery.newIntRange(key, value, value, true, true);
        }
        queries.add(query);
        return this;
    }

    public QueryBuilder addField(String key, List<String> values, FilterOperator op) throws ZepException {
        if ( values.size() == 0 ) {
            throw new ZepException("You can not search on an empty list.");
        }

        final BooleanClause.Occur occur;
        final BooleanQuery booleanQuery = new BooleanQuery();
        if (op == FilterOperator.AND) {
            occur = BooleanClause.Occur.MUST;
        }
        else {
            occur = BooleanClause.Occur.SHOULD;
        }

        for (String value : values) {
            booleanQuery.add(new TermQuery(new Term(key, value)), occur);
        }
        queries.add(booleanQuery);
        return this;
    }

    public QueryBuilder addFieldOfIntegers(String key, List<Integer> values) throws ZepException {
        if ( values.size() == 0 ) {
            throw new ZepException("You can not search on an empty list.");
        }

        final BooleanClause.Occur occur = BooleanClause.Occur.SHOULD;
        final BooleanQuery booleanQuery = new BooleanQuery();

        for (int value : values) {
            booleanQuery.add(NumericRangeQuery.newIntRange(key, value, value, true, true), occur);
        }
        queries.add(booleanQuery);
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

    public QueryBuilder addFieldOfEnumNames(String key, List<? extends Enum<?>> values) throws ZepException {
        List<String> strValues = new ArrayList<String>(values.size());
        for ( Enum<?> e : values ) {
            strValues.add(e.name());
        }
        addField(key, strValues, FilterOperator.OR);
        return this;
    }

    public QueryBuilder addRange(String key, Integer from, Integer to) {
        this.queries.add(NumericRangeQuery.newIntRange(key, from, to, true, true));
        return this;
    }

    public QueryBuilder addRange(String key, Long from, Long to) {
        this.queries.add(NumericRangeQuery.newLongRange(key, from, to, true, true));
        return this;
    }

    public Query build() {
        if (this.queries.isEmpty()) {
            return new MatchAllDocsQuery();
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
