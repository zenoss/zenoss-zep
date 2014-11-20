/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2010-2011, 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/
package org.zenoss.zep.index.impl;

import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.ZepUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class BaseEventIndexMapper {
    protected static byte[] compressProtobuf(EventSummary eventSummary) throws ZepException {
        final byte[] uncompressed = eventSummary.toByteArray();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(uncompressed.length);
        GZIPOutputStream gzos = null;
        try {
            gzos = new GZIPOutputStream(baos);
            gzos.write(uncompressed);
            gzos.finish();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        } finally {
            if (gzos != null) {
                ZepUtils.close(gzos);
            }
        }
    }

    protected static EventSummary uncompressProtobuf(byte[] compressed) throws ZepException {
        ByteArrayInputStream bais = new ByteArrayInputStream(compressed);
        GZIPInputStream gzis = null;
        try {
            gzis = new GZIPInputStream(bais);
            return EventSummary.newBuilder().mergeFrom(gzis).build();
        } catch (IOException e) {
            throw new ZepException(e.getLocalizedMessage(), e);
        } finally {
            if (gzis != null) {
                ZepUtils.close(gzis);
            }
        }
    }

}
