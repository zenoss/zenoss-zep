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

    public static final String DETAILS_KEY_PREFIX = "CZ_EVENT_DETAIL-";
    public static final String SOURCE_KEY = "source";
    public static final String SOURCE_TYPE_KEY = "source-type";
    public static final String SOURCE_VENDOR_KEY = "source-vendor";
    public static final String FINGERPRINT_KEY = "fingerprint";
    public static final String UUID_KEY = "uuid";
    public static final String SEVERITY_KEY = "severity";
    public static final String PARENT_CONTEXT_UUID_KEY = "parentContextUUID";
    public static final String PARENT_CONTEXT_ID_KEY = "parentContextIdentifier";
    public static final String PARENT_CONTEXT_TITLE_KEY = "parentContextTitle";
    public static final String PARENT_CONTEXT_TYPE_KEY = "parentContextType";
    public static final String CONTEXT_UUID_KEY = "contextUUID";
    public static final String CONTEXT_ID_KEY = "contextIdentifier";
    public static final String CONTEXT_TITLE_KEY = "contextTitle";
    public static final String CONTEXT_TYPE_KEY = "contextType";
    public static final String MESSAGE_KEY = "message";
    public static final String SUMMARY_KEY = "summary";
    public static final String MONITOR_KEY = "monitor";
    public static final String AGENT_KEY = "agent";
    public static final String EVENT_KEY_KEY = "eventKey";
    public static final String EVENT_CLASS_KEY = "eventClass";
    public static final String EVENT_CLASS_KEY_KEY = "eventClassKey";
    public static final String EVENT_CLASS_MAPPING_KEY = "eventClassMappingUuid";
    public static final String EVENT_GROUP_KEY = "eventGroup";
    public static final String COUNT_KEY = "count";
    public static final String FIRST_SEEN_KEY = "firstSeen";
    public static final String LAST_SEEN_KEY = "lastSeen";
    public static final String UPDATE_TIME_KEY = "updateTime";
    public static final String CLEARED_BY_KEY = "clearedBy";
    public static final String STATUS_KEY = "eventStatus";

    public static final String SOURCE_TYPE = "CZ";
    public static final String SOURCE_VENDOR = "Zenoss";

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

        //List<Any> anyValues = ;
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



