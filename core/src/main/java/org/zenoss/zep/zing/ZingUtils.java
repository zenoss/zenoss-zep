/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2018, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.zing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.zing.proto.cloud.common.Scalar;
import org.zenoss.zing.proto.cloud.common.ScalarArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ZingUtils {
    private static final Logger logger = LoggerFactory.getLogger(ZingUtils.class);

    // The methods below have been brought from the dataflow-common.utils
    // Any issues found should be ported to dataflow-common and vice-versa

    public static boolean isNullOrEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    public static String sanitizeToken(String token) {
        String sanitizedToken = token;

        if (!ZingUtils.isNullOrEmpty(token)) {
            sanitizedToken = sanitizedToken.trim().toUpperCase();
        }
        return sanitizedToken;
    }

    /**
     * Convert a Java Object into a protobufs Scalar value.  Only supports the scalar types; returns null if the
     * type can not be converted.
     *
     * @param childObj
     * @return
     */
    public static Scalar getScalarValueFromObject(final Object childObj) {
        Scalar scalarValue = null;
        if (childObj instanceof String) {
            scalarValue = Scalar.newBuilder().setStringVal((String) childObj).build();
        } else if (childObj instanceof Boolean) {
            scalarValue = Scalar.newBuilder().setBoolVal((Boolean) childObj).build();
        } else if (childObj instanceof Integer) {
            scalarValue = Scalar.newBuilder().setLongVal(((Integer) childObj).longValue()).build();
        } else if (childObj instanceof Long) {
            scalarValue = Scalar.newBuilder().setLongVal((Long) childObj).build();
        } else if (childObj instanceof Float) {
            scalarValue = Scalar.newBuilder().setFloatVal((Float) childObj).build();
        } else if (childObj instanceof Double) {
            scalarValue = Scalar.newBuilder().setDoubleVal((Double) childObj).build();
        }

        return scalarValue;
    }

    /**
     * Convert a list of Object to a ScalarArray
     *
     * @param values
     * @return ScalarArray of Scalar equivalents of values
     */
    public static ScalarArray getScalarArrayFromList(final List<?> values) {
        final ScalarArray.Builder scalarArray = ScalarArray.newBuilder();
        for (Object oneValue: values) {
            final Scalar scalarValue = ZingUtils.getScalarValueFromObject(oneValue);
            if (scalarValue != null) {
                scalarArray.addScalars(scalarValue);
            }
        }

        return scalarArray.build();
    }

    /**
     * Convert one or more Object to a ScalarArray
     *
     * @param obs
     *            One or more Object to be converted
     * @return ScalarArray of Scalar equivalents of objs
     */
    public static ScalarArray getScalarArray(Object ...obs) {
        return getScalarArrayFromList(Arrays.asList(obs));
    }

    /**
     * Get a Java scalar object from a protobuf Scalar object.
     *
     * @param scalarValue
     * @return null if the Scalar object is not a scalar.
     */
    public static Object getObjectFromScalarValue(final Scalar scalarValue) {
        Object value = null;
        switch (scalarValue.getValueCase()) {
            case LONG_VAL: {
                value = scalarValue.getLongVal();
                break;
            }
            case ULONG_VAL: {
                value = scalarValue.getUlongVal();
                break;
            }
            case UINT_VAL: {
                value = scalarValue.getUintVal();
                break;
            }
            case FLOAT_VAL: {
                value = scalarValue.getFloatVal();
                break;
            }
            case DOUBLE_VAL: {
                value = scalarValue.getDoubleVal();
                break;
            }
            case STRING_VAL: {
                value = scalarValue.getStringVal();
                break;
            }
            case BOOL_VAL: {
                value = scalarValue.getBoolVal();
                break;
            }
            case VALUE_NOT_SET: {
                break;
            }
            default: {
                break;
            }
        }
        return value;
    }

    /**
     * Get a list a Java Objects from an ScalarArray.
     *
     * Skips entries in the ScalarArray which do not represent a scalar value.
     *
     * @param key only used for log messages
     * @param scalarArray
     * @return
     */
    public static List<Object> getListFromScalarArray(final String key, final ScalarArray scalarArray) {
        final List<Object> values = new ArrayList<>();

        if (scalarArray == null) {
            return values;
        }

        for(Scalar scalarValue : scalarArray.getScalarsList()) {
            final Object value = getObjectFromScalarValue(scalarValue);
            if (value == null) {
                logger.warn("Skipping value for key='%s' because type is not supported", key);
            } else {
                values.add(value);
            }
        }
        return values;
    }
}
