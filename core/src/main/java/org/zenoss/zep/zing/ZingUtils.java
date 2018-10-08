/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2018, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.zing;

import com.google.protobuf.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.zing.proto.model.AnyArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ZingUtils {
    private static final Logger logger = LoggerFactory.getLogger(ZingUtils.class);

    // The methods below have been brought from the dataflow-common.utils
    // Any issues found should be ported to dataflow-common and vice-versa

    /**
     * Convert a Java Object into a protobufs Any value.  Only supports the scalar types; returns null if the
     * type can not be converted.
     *
     * @param childObj
     * @return
     */
    public static Any getAnyValueFromObject(final Object childObj) {
        Any anyValue = null;
        if (childObj instanceof String) {
            anyValue = Any.pack(StringValue.newBuilder().setValue((String) childObj).build());
        } else if (childObj instanceof Boolean) {
            anyValue = Any.pack(BoolValue.newBuilder().setValue((Boolean)childObj).build());
        } else if (childObj instanceof Integer) {
            // Always use Longs
            anyValue = Any.pack(Int64Value.newBuilder().setValue(((Integer) childObj).longValue()).build());
        } else if (childObj instanceof Long) {
            anyValue = Any.pack(Int64Value.newBuilder().setValue((Long)childObj).build());
        } else if (childObj instanceof Float) {
            anyValue = Any.pack(FloatValue.newBuilder().setValue((Float)childObj).build());
        } else if (childObj instanceof Double) {
            anyValue = Any.pack(DoubleValue.newBuilder().setValue((Double)childObj).build());
        }
        return anyValue;
    }

    /**
     * Convert a list of Object to an AnyArray
     *
     * @param values
     * @return AnyArray of Any equivalents of values
     */
    public static AnyArray getAnyArrayFromList(final List<Object> values) {
        final AnyArray.Builder anyArray = AnyArray.newBuilder();
        for (Object oneValue: values) {
            final Any anyValue = ZingUtils.getAnyValueFromObject(oneValue);
            if (anyValue != null) {
                anyArray.addValue(anyValue);
            }
        }
        return anyArray.build();
    }

    /**
     * Convert one or more Object to an AnyArray
     *
     * @param obs
     *            One or more Object to be converted
     * @return AnyArray of Any equivalents of objs
     */
    public static AnyArray getAnyArray(Object ...obs) {
        return getAnyArrayFromList(Arrays.asList(obs));
    }

    /**
     * Get a Java scalar object from a protobuf Any object.
     *
     * @param anyValue
     * @return null if the Any object is not a scalar.
     */
    public static Object getObjectFromAnyValue(final Any anyValue) {
        Object value = null;
        try {
            if (anyValue.is(StringValue.class)) {
                value = anyValue.unpack(StringValue.class).getValue();
            } else if (anyValue.is(BoolValue.class)) {
                value = anyValue.unpack(BoolValue.class).getValue();
            } else if (anyValue.is(Int32Value.class)) {
                // Always use longs
                value = anyValue.unpack(Int32Value.class).getValue();
                value = ((Integer) value).longValue();
            } else if (anyValue.is(Int64Value.class)) {
                value = anyValue.unpack(Int64Value.class).getValue();
            } else if (anyValue.is(FloatValue.class)) {
                value = anyValue.unpack(FloatValue.class).getValue();
            } else if (anyValue.is(DoubleValue.class)) {
                value = anyValue.unpack(DoubleValue.class).getValue();
            }
        }
        catch (InvalidProtocolBufferException e) {
            value = null;
        }
        return value;
    }

    /**
     * Get a list a Java Objects from an AnyArray.
     *
     * Skips entries in the AnyArray which do not represent a scalar value.
     *
     * @param key only used for log messages
     * @param anyArray
     * @return
     */
    public static List<Object> getListFromAnyArray(final String key, final AnyArray anyArray) {
        final List<Object> values = new ArrayList<>();

        if (anyArray == null) {
            return values;
        }

        for(Any anyValue : anyArray.getValueList()) {
            final Object value = getObjectFromAnyValue(anyValue);
            if (value == null) {
                logger.warn("Skipping value for key='%s' because type={} is not supported",
                        key, anyValue.getTypeUrl());
            } else {
                values.add(value);
            }
        }
        return values;
    }

    public static boolean isNullOrEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

}



