/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2018, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.zing;

import org.junit.Test;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.zing.proto.cloud.common.Scalar;
import org.zenoss.zing.proto.cloud.common.ScalarArray;

import org.zenoss.zing.proto.event.Event;
import org.zenoss.zing.proto.event.Status;
import org.zenoss.zing.proto.event.Severity;

import com.google.protobuf.BoolValue;

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
        Scalar scalarValue = protoEvent.getDimensionsOrDefault(ZingConstants.SOURCE_KEY, null);
        assertEquals((String) ZingUtils.getObjectFromScalarValue(scalarValue), src);
        assertTrue(protoEvent.containsDimensions(ZingConstants.FINGERPRINT_KEY));
        scalarValue = protoEvent.getDimensionsOrDefault(ZingConstants.FINGERPRINT_KEY, null);
        assertEquals((String) ZingUtils.getObjectFromScalarValue(scalarValue), fingerprint);
        assertTrue(protoEvent.containsDimensions(ZingConstants.UUID_KEY));
        scalarValue = protoEvent.getDimensionsOrDefault(ZingConstants.UUID_KEY, null);
        assertEquals((String) ZingUtils.getObjectFromScalarValue(scalarValue), eventUUID);

        List<Object> mdValues;
        ScalarArray scalarArr;
        // severity
        protoEvent.containsMetadata(ZingConstants.SEVERITY_KEY);
        scalarArr = protoEvent.getMetadataOrDefault(ZingConstants.SEVERITY_KEY, null);
        mdValues = ZingUtils.getListFromScalarArray(ZingConstants.SEVERITY_KEY, scalarArr);
        assertTrue(mdValues.size()==1);
        assertEquals((String)mdValues.get(0), severity);
        // contextUUID
        protoEvent.containsMetadata(ZingConstants.CONTEXT_UUID_KEY);
        scalarArr = protoEvent.getMetadataOrDefault(ZingConstants.CONTEXT_UUID_KEY, null);
        mdValues = ZingUtils.getListFromScalarArray(ZingConstants.CONTEXT_UUID_KEY, scalarArr);
        assertTrue(mdValues.size()==1);
        assertEquals((String)mdValues.get(0), contextUUID);
        // parentContextUUID
        protoEvent.containsMetadata(ZingConstants.PARENT_CONTEXT_UUID_KEY);
        scalarArr = protoEvent.getMetadataOrDefault(ZingConstants.PARENT_CONTEXT_UUID_KEY, null);
        mdValues = ZingUtils.getListFromScalarArray(ZingConstants.PARENT_CONTEXT_UUID_KEY, scalarArr);
        assertTrue(mdValues.size()==1);
        assertEquals((String)mdValues.get(0), parentContextUUID);
        // detail
        String mdDetailKey = ZingConstants.DETAILS_KEY_PREFIX + detailKey;
        protoEvent.containsMetadata(mdDetailKey);
        scalarArr = protoEvent.getMetadataOrDefault(mdDetailKey, null);
        mdValues = ZingUtils.getListFromScalarArray(mdDetailKey, scalarArr);
        assertTrue(mdValues.size()==1);
        assertEquals((String)mdValues.get(0), detailValue);
    }

    @Test
    public void testZingEventToZingProtoStatus() {
        String tnt = "acme";
        String src = "austin_tx";
        String parentContextUUID = "parent_context_uuid";
        String contextUUID = "context_uuid";
        String eventUUID = "event_uuid";
        String fingerprint = "event_fingerprint_123";
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

        String status = "NEW";
        builder.setStatus(status);
        Event protoEvent = builder.build().toZingEventProto();
        assertEquals(protoEvent.getStatus(), Status.STATUS_OPEN);
        // Status of ACKNOWLEDGED implies falsey Event.getAcknowledged()
        assertTrue(protoEvent.hasAcknowledged());
        assertFalse(protoEvent.getAcknowledged().getValue());

        status = "ACKNOWLEDGED";
        builder.setStatus(status);
        protoEvent = builder.build().toZingEventProto();
        assertEquals(protoEvent.getStatus(), Status.STATUS_OPEN);
        // Status of ACKNOWLEDGED implies Event.getAcknowledged()
        assertTrue(protoEvent.hasAcknowledged());
        assertTrue(protoEvent.getAcknowledged().getValue());


        status = "SUPPRESSED";
        builder.setStatus(status);
        protoEvent = builder.build().toZingEventProto();
        assertEquals(protoEvent.getStatus(), Status.STATUS_SUPPRESSED);

        status = "CLOSED";
        builder.setStatus(status);
        protoEvent = builder.build().toZingEventProto();
        assertEquals(protoEvent.getStatus(), Status.STATUS_CLOSED);

        status = "CLEARED";
        builder.setStatus(status);
        protoEvent = builder.build().toZingEventProto();
        assertEquals(protoEvent.getStatus(), Status.STATUS_CLOSED);

        status = "DROPPED";
        builder.setStatus(status);
        protoEvent = builder.build().toZingEventProto();
        assertEquals(protoEvent.getStatus(), Status.STATUS_CLOSED);

        status = "AGED";
        builder.setStatus(status);
        protoEvent = builder.build().toZingEventProto();
        assertEquals(protoEvent.getStatus(), Status.STATUS_CLOSED);

    }

    @Test
    public void testZingEventToZingProtoSeverity() {
        String tnt = "acme";
        String src = "austin_tx";
        String parentContextUUID = "parent_context_uuid";
        String contextUUID = "context_uuid";
        String eventUUID = "event_uuid";
        String fingerprint = "event_fingerprint_123";
        String status = "ACKNOWLEDGED";
        String detailKey = "bla";
        String detailValue = "blabla";

        long ts = System.currentTimeMillis();
        ZingEvent.Builder builder = new ZingEvent.Builder(tnt, src, ts);
        builder.setUuid(eventUUID);
        builder.setFingerprint(fingerprint);
        builder.setContextUUID(contextUUID);
        builder.setStatus(status);
        builder.setParentContextUUID(parentContextUUID);
        builder.setDetail(detailKey, Arrays.asList(detailValue));

        // Severity of CLEAR implies Severity.SEVERITY_DEFAULT
        // It also implese Status.STATUS_CLOSED
        String severity = "CLEAR";
        builder.setSeverity(severity);
        Event protoEvent = builder.build().toZingEventProto();
        assertEquals(protoEvent.getSeverity(), Severity.SEVERITY_DEFAULT);
        assertEquals(protoEvent.getStatus(), Status.STATUS_CLOSED);

        // Severity of CLEAR implies Severity.SEVERITY_DEFAULT
        severity = "DEBUG";
        builder.setSeverity(severity);
        protoEvent = builder.build().toZingEventProto();
        assertEquals(protoEvent.getSeverity(), Severity.SEVERITY_DEBUG);

        // Severity of CLEAR implies Severity.SEVERITY_DEFAULT
        severity = "INFO";
        builder.setSeverity(severity);
        protoEvent = builder.build().toZingEventProto();
        assertEquals(protoEvent.getSeverity(), Severity.SEVERITY_INFO);

        // Severity of CLEAR implies Severity.SEVERITY_DEFAULT
        severity = "WARNING";
        builder.setSeverity(severity);
        protoEvent = builder.build().toZingEventProto();
        assertEquals(protoEvent.getSeverity(), Severity.SEVERITY_WARNING);

        // Severity of CLEAR implies Severity.SEVERITY_DEFAULT
        severity = "ERROR";
        builder.setSeverity(severity);
        protoEvent = builder.build().toZingEventProto();
        assertEquals(protoEvent.getSeverity(), Severity.SEVERITY_ERROR);

        // Severity of CLEAR implies Severity.SEVERITY_DEFAULT
        severity = "CRITICAL";
        builder.setSeverity(severity);
        protoEvent = builder.build().toZingEventProto();
        assertEquals(protoEvent.getSeverity(), Severity.SEVERITY_CRITICAL);
    }

    @Test
    public void testZingEventToZingProtoMisc() {
        // Test (setSummary, setBody, setType) setters for zing.proto.event
        String tnt = "acme";
        String src = "austin_tx";
        String contextUUID = "context_uuid";
        String eventUUID = "event_uuid";
        String fingerprint = "event_fingerprint_123";
        String severity = "INFO";
        String status = "NEW";
        String summary = "This is a sample summary!";
        String message = "This is a sample message!";
        String eventClassKey = "TheSampleEventClassKey";

        long ts = System.currentTimeMillis();
        ZingEvent.Builder builder = new ZingEvent.Builder(tnt, src, ts);
        builder.setUuid(eventUUID);
        builder.setFingerprint(fingerprint);
        builder.setContextUUID(contextUUID);

        // Set miscellaneous properties on EventBuilder
        builder.setSeverity(severity);
        builder.setMessage(message);
        builder.setSummary(summary);
        builder.setEventClassKey(eventClassKey);

        // Test miscellaneous properties on EventBuilder
        Event protoEvent = builder.build().toZingEventProto();
        assertEquals(protoEvent.getSummary(), summary);
        assertEquals(protoEvent.getBody(), message);
        assertEquals(protoEvent.getType(), eventClassKey);
        assertFalse(protoEvent.hasAcknowledged());
    }
}
