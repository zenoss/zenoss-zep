package org.zenoss.zep.index.impl;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;

public final class NaturalStringValueComparator extends FieldComparator<String> {

    private String[] values;
    private String[] currentReaderValues;
    private final String field;
    private String bottom;
    private NaturalOrderComparator comparator;

    public NaturalStringValueComparator(int numHits, String field) {
        values = new String[numHits];
        this.field = field;
        this.comparator = new NaturalOrderComparator();
    }

    @Override
    public int compare(int slot1, int slot2) {
        final String val1 = values[slot1];
        final String val2 = values[slot2];
        if (val1 == null) {
            if (val2 == null) {
                return 0;
            }
            return -1;
        } else if (val2 == null) {
            return 1;
        }

        return comparator.compare( val1, val2);
    }

    @Override
    public int compareBottom(int doc) {
        final String val2 = currentReaderValues[doc];
        if (bottom == null) {
            if (val2 == null) {
                return 0;
            }
            return -1;
        } else if (val2 == null) {
            return 1;
        }

        return comparator.compare( bottom, val2);
    }

    @Override
    public void copy(int slot, int doc) {
        values[slot] = currentReaderValues[doc];
    }

    @Override
    public void setNextReader(IndexReader reader, int docBase) throws IOException {
        currentReaderValues = FieldCache.DEFAULT.getStrings(reader, field);
    }

    @Override
    public void setBottom(final int bottom) {
        this.bottom = values[bottom];
    }

    @Override
    public String value(int slot) {
        return values[slot];
    }

    @Override
    public int compareValues(String val1, String val2) {
        if (val1 == null) {
            if (val2 == null) {
                return 0;
            }
            return -1;
        } else if (val2 == null) {
            return 1;
        } else {
            return comparator.compare( val1, val2);
        }
    }
}
