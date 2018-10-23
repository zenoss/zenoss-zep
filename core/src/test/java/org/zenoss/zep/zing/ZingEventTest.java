/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2018, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.zing;

import com.google.protobuf.Any;
import org.junit.Test;
import org.zenoss.zing.proto.event.Event;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.zing.proto.model.AnyArray;

public class ZingEventTest {

    private static final Logger logger = LoggerFactory.getLogger(ZingEventTest.class);

    @Test
    public void testValidateEvent() {
        String tnt = "acme";
        String src = "aus";
        long ts = System.currentTimeMillis();

        // validate mandatory fields
        ZingEvent badEvent = new ZingEvent.Builder("", src, ts).build();
        assertFalse(badEvent.isValid());
        badEvent = new ZingEvent.Builder(tnt, "", ts).build();
        assertFalse(badEvent.isValid());
        badEvent = new ZingEvent.Builder(tnt, src, 0).build();
        assertFalse(badEvent.isValid());
        assertEquals(badEvent.toZingEventProto(), null);

        ZingEvent.Builder builder = new ZingEvent.Builder(tnt, src, ts);
        builder.setUuid("123");
        builder.setSeverity("INFO");
        builder.setFingerprint("abc");
        builder.setContextUUID("abc");
        ZingEvent validEvent = builder.build();
        assertTrue(validEvent.isValid());
        // no uuid
        builder.setUuid("");
        badEvent = builder.build();
        assertFalse(badEvent.isValid());
        builder.setUuid("123");
        // no sev
        builder.setSeverity("");
        badEvent = builder.build();
        assertFalse(badEvent.isValid());
        builder.setSeverity("INFO");
        // no fingerprint
        builder.setFingerprint("");
        badEvent = builder.build();
        assertFalse(badEvent.isValid());
        builder.setFingerprint("abc");
        // no contextUUID
        builder.setContextUUID("");
        badEvent = builder.build();
        assertFalse(badEvent.isValid());
        builder.setContextUUID("abc");
        assertTrue(builder.build().isValid());
    }

    @Test
    public void testValidateZingEventToZingProto() {
        String tnt = "acme";
        String src = "aus";
        String parentContextUUID = "parent_context_uuid";
        String contextUUID = "context_uuid";
        String eventUUID = "event_uuid";
        String fingerprint = "event_fingerprint";
        String severity = "INFO";
        String detailKey = "bla";
        String detailValue = "blabla";

        long ts = System.currentTimeMillis();
        ZingEvent.Builder builder = new ZingEvent.Builder(tnt, src, ts);
        builder.setUuid(eventUUID);
        builder.setFingerprint(fingerprint);
        builder.setContextUUID(contextUUID);
        builder.setSeverity(severity);
        builder.setParentContextUUID(parentContextUUID);
        builder.setDetail(detailKey, Arrays.asList(detailValue));

        Event protoEvent = builder.build().toZingEventProto();
        assertTrue(protoEvent.getTenant() == tnt);
        assertTrue(protoEvent.getName() == "");
        assertTrue(protoEvent.getTimestamp() == ts);

        assertTrue(protoEvent.containsDimensions(ZingConstants.SOURCE_KEY));
        Any anyValue = protoEvent.getDimensionsOrDefault(ZingConstants.SOURCE_KEY, null);
        assertEquals((String) ZingUtils.getObjectFromAnyValue(anyValue), src);
        assertTrue(protoEvent.containsDimensions(ZingConstants.FINGERPRINT_KEY));
        anyValue = protoEvent.getDimensionsOrDefault(ZingConstants.FINGERPRINT_KEY, null);
        assertEquals((String) ZingUtils.getObjectFromAnyValue(anyValue), fingerprint);
        assertTrue(protoEvent.containsDimensions(ZingConstants.UUID_KEY));
        anyValue = protoEvent.getDimensionsOrDefault(ZingConstants.UUID_KEY, null);
        assertEquals((String) ZingUtils.getObjectFromAnyValue(anyValue), eventUUID);

        List<Object> mdValues;
        AnyArray anyArr;
        // severity
        protoEvent.containsMetadata(ZingConstants.SEVERITY_KEY);
        anyArr = protoEvent.getMetadataOrDefault(ZingConstants.SEVERITY_KEY, null);
        mdValues = ZingUtils.getListFromAnyArray(ZingConstants.SEVERITY_KEY, anyArr);
        assertTrue(mdValues.size()==1);
        assertEquals((String)mdValues.get(0), severity);
        // contextUUID
        protoEvent.containsMetadata(ZingConstants.CONTEXT_UUID_KEY);
        anyArr = protoEvent.getMetadataOrDefault(ZingConstants.CONTEXT_UUID_KEY, null);
        mdValues = ZingUtils.getListFromAnyArray(ZingConstants.CONTEXT_UUID_KEY, anyArr);
        assertTrue(mdValues.size()==1);
        assertEquals((String)mdValues.get(0), contextUUID);
        // parentContextUUID
        protoEvent.containsMetadata(ZingConstants.PARENT_CONTEXT_UUID_KEY);
        anyArr = protoEvent.getMetadataOrDefault(ZingConstants.PARENT_CONTEXT_UUID_KEY, null);
        mdValues = ZingUtils.getListFromAnyArray(ZingConstants.PARENT_CONTEXT_UUID_KEY, anyArr);
        assertTrue(mdValues.size()==1);
        assertEquals((String)mdValues.get(0), parentContextUUID);
        // detail
        String mdDetailKey = ZingConstants.DETAILS_KEY_PREFIX + detailKey;
        protoEvent.containsMetadata(mdDetailKey);
        anyArr = protoEvent.getMetadataOrDefault(mdDetailKey, null);
        mdValues = ZingUtils.getListFromAnyArray(mdDetailKey, anyArr);
        assertTrue(mdValues.size()==1);
        assertEquals((String)mdValues.get(0), detailValue);
    }
}
