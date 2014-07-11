/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/


package org.zenoss.zep.index.impl;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.protobufs.zep.Zep.EventDetail;
import org.zenoss.protobufs.zep.Zep.EventDetailItem;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventTag;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.ZepInstance;
import org.zenoss.zep.ZepUtils;
import org.zenoss.zep.index.IndexedDetailsConfiguration;
import org.zenoss.zep.utils.IpUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.zenoss.zep.index.impl.IndexConstants.FIELD_AGENT;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_COUNT;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_CURRENT_USER_NAME;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_ELEMENT_IDENTIFIER;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_ELEMENT_IDENTIFIER_NOT_ANALYZED;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_ELEMENT_SUB_IDENTIFIER;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_ELEMENT_SUB_IDENTIFIER_NOT_ANALYZED;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_ELEMENT_SUB_TITLE;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_ELEMENT_SUB_TITLE_NOT_ANALYZED;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_ELEMENT_TITLE;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_ELEMENT_TITLE_NOT_ANALYZED;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_EVENT_CLASS;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_EVENT_CLASS_KEY;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_EVENT_CLASS_NOT_ANALYZED;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_EVENT_GROUP;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_EVENT_KEY;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_FINGERPRINT;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_FIRST_SEEN_TIME;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_LAST_SEEN_TIME;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_MESSAGE;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_MONITOR;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_PROTOBUF;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_SEVERITY;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_STATUS;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_STATUS_CHANGE_TIME;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_SUMMARY;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_SUMMARY_NOT_ANALYZED;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_TAGS;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_UPDATE_TIME;
import static org.zenoss.zep.index.impl.IndexConstants.FIELD_UUID;
import static org.zenoss.zep.index.impl.IndexConstants.IP_ADDRESS_TYPE_4;
import static org.zenoss.zep.index.impl.IndexConstants.IP_ADDRESS_TYPE_6;
import static org.zenoss.zep.index.impl.IndexConstants.IP_ADDRESS_TYPE_SUFFIX;
import static org.zenoss.zep.index.impl.IndexConstants.LUCENE_VERSION;
import static org.zenoss.zep.index.impl.IndexConstants.SORT_SUFFIX;

public class EventIndexMapper {

    public static Analyzer createAnalyzer() {
        Map<String, Analyzer> fieldAnalyzers = new HashMap<String, Analyzer>();
        fieldAnalyzers.put(FIELD_ELEMENT_IDENTIFIER, new IdentifierAnalyzer());
        fieldAnalyzers.put(FIELD_ELEMENT_SUB_IDENTIFIER, new IdentifierAnalyzer());
        fieldAnalyzers.put(FIELD_ELEMENT_TITLE, new IdentifierAnalyzer());
        fieldAnalyzers.put(FIELD_ELEMENT_SUB_TITLE, new IdentifierAnalyzer());
        fieldAnalyzers.put(FIELD_SUMMARY, new SummaryAnalyzer());
        fieldAnalyzers.put(FIELD_EVENT_CLASS, new PathAnalyzer());
        fieldAnalyzers.put(FIELD_MESSAGE, new SummaryAnalyzer());
        return new PerFieldAnalyzerWrapper(new KeywordAnalyzer(), fieldAnalyzers);
    }

    public static IndexWriterConfig createIndexWriterConfig(Analyzer analyzer, ZepInstance zepInstance) {
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(LUCENE_VERSION, analyzer);
        Map<String, String> cfg = zepInstance.getConfig();
        String ramBufferSizeMb = cfg.get("zep.index.ram_buffer_size_mb");
        if (ramBufferSizeMb != null) {
            try {
                indexWriterConfig.setRAMBufferSizeMB(Double.valueOf(ramBufferSizeMb.trim()));
            } catch (NumberFormatException nfe) {
                logger.warn("Invalid value for zep.index.ram_buffer_size_mb: {}", ramBufferSizeMb);
            }
        }
        return indexWriterConfig;
    }

    private static byte[] compressProtobuf(EventSummary eventSummary) throws ZepException {
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

    private static EventSummary uncompressProtobuf(byte[] compressed) throws ZepException {
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

    public static final String DETAIL_INDEX_PREFIX = "details.";

    private static final Logger logger = LoggerFactory.getLogger(EventIndexMapper.class);

    public static Document fromEventSummary(IndexedDetailsConfiguration eventDetailsConfigDao, EventSummary summary, Map<String, EventDetailItem> detailsConfig,
                                            boolean isArchive) throws ZepException {
        Document doc = new Document();

        // Store the entire serialized protobuf so we can reproduce the entire event from the index.
        // Archive events don't store serialized protobufs - see ZEN-2159
        if (!isArchive) {
            doc.add(new Field(FIELD_PROTOBUF, compressProtobuf(summary)));
        }

        // Store the UUID for more lightweight queries against the index
        doc.add(new Field(FIELD_UUID, summary.getUuid(), Store.YES, Index.NOT_ANALYZED_NO_NORMS));

        doc.add(new Field(FIELD_CURRENT_USER_NAME, summary.getCurrentUserName(), Store.NO,
                Index.NOT_ANALYZED_NO_NORMS));

        doc.add(new IntField(FIELD_STATUS, summary.getStatus().getNumber(), Store.YES));
        doc.add(new IntField(FIELD_COUNT, summary.getCount(), Store.YES));
        doc.add(new LongField(FIELD_LAST_SEEN_TIME, summary.getLastSeenTime(), Store.NO));
        doc.add(new LongField(FIELD_FIRST_SEEN_TIME, summary.getFirstSeenTime(), Store.NO));
        doc.add(new LongField(FIELD_STATUS_CHANGE_TIME, summary.getStatusChangeTime(), Store.NO));
        doc.add(new LongField(FIELD_UPDATE_TIME, summary.getUpdateTime(), Store.NO));

        Event event = summary.getOccurrence(0);
        doc.add(new Field(FIELD_FINGERPRINT, event.getFingerprint(), Store.NO, Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new Field(FIELD_SUMMARY, event.getSummary(), Store.NO, Index.ANALYZED_NO_NORMS));
        doc.add(new Field(FIELD_SUMMARY_NOT_ANALYZED, event.getSummary().toLowerCase(), Store.NO, Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new IntField(FIELD_SEVERITY, event.getSeverity().getNumber(), Store.YES));

        doc.add(new Field(FIELD_EVENT_CLASS, event.getEventClass(), Store.NO, Index.ANALYZED_NO_NORMS));
        // Store with a trailing slash to make lookups simpler
        doc.add(new Field(FIELD_EVENT_CLASS_NOT_ANALYZED, event.getEventClass().toLowerCase() + "/", Store.NO,
                Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new Field(FIELD_AGENT, event.getAgent(), Store.NO, Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new Field(FIELD_MONITOR, event.getMonitor(), Store.NO, Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new Field(FIELD_EVENT_KEY, event.getEventKey(), Store.NO, Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new Field(FIELD_EVENT_CLASS_KEY, event.getEventClassKey(), Store.NO, Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new Field(FIELD_EVENT_GROUP, event.getEventGroup(), Store.NO, Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new Field(FIELD_MESSAGE, event.getMessage(), Store.NO, Index.ANALYZED_NO_NORMS));

        for (EventTag tag : event.getTagsList()) {
            for (String tagUuid : tag.getUuidList()) {
                doc.add(new Field(FIELD_TAGS, tagUuid, Store.YES, Index.NOT_ANALYZED_NO_NORMS));
            }
        }

        EventActor actor = event.getActor();
        String uuid = actor.getElementUuid();
        if (uuid != null && !uuid.isEmpty()) {
            doc.add(new Field(FIELD_TAGS, uuid, Store.YES, Index.NOT_ANALYZED_NO_NORMS));
        }

        String id = actor.getElementIdentifier();
        doc.add(new Field(FIELD_ELEMENT_IDENTIFIER, id, Store.NO, Index.ANALYZED_NO_NORMS));
        doc.add(new Field(FIELD_ELEMENT_IDENTIFIER_NOT_ANALYZED, id.toLowerCase(), Store.NO, Index.NOT_ANALYZED_NO_NORMS));

        String title = actor.getElementTitle();
        doc.add(new Field(FIELD_ELEMENT_TITLE, title, Store.NO, Index.ANALYZED_NO_NORMS));
        doc.add(new Field(FIELD_ELEMENT_TITLE_NOT_ANALYZED, title.toLowerCase(), Store.NO, Index.NOT_ANALYZED_NO_NORMS));

        String subUuid = actor.getElementSubUuid();
        if (subUuid != null && !subUuid.isEmpty()) {
            doc.add(new Field(FIELD_TAGS, subUuid, Store.YES, Index.NOT_ANALYZED_NO_NORMS));
        }

        String subId = actor.getElementSubIdentifier();
        doc.add(new Field(FIELD_ELEMENT_SUB_IDENTIFIER, subId, Store.NO, Index.ANALYZED_NO_NORMS));
        doc.add(new Field(FIELD_ELEMENT_SUB_IDENTIFIER_NOT_ANALYZED, subId.toLowerCase(), Store.NO, Index.NOT_ANALYZED_NO_NORMS));

        String subTitle = actor.getElementSubTitle();
        doc.add(new Field(FIELD_ELEMENT_SUB_TITLE, subTitle, Store.NO, Index.ANALYZED_NO_NORMS));
        doc.add(new Field(FIELD_ELEMENT_SUB_TITLE_NOT_ANALYZED, subTitle.toLowerCase(), Store.NO, Index.NOT_ANALYZED_NO_NORMS));
        // find details  for indexing
        List<EventDetail> evtDetails = event.getDetailsList();
        
        // default all the details to unprintable ascii character so we can
        // search for None's. That character was chosen because it doesn't take much space
        Map<String, EventDetailItem> allDetails = eventDetailsConfigDao.getEventDetailItemsByName();
        Iterator it = allDetails.entrySet().iterator();
        while (it.hasNext()) {
            boolean found = false;
            Map.Entry entry = (Map.Entry) it.next();
            // make sure that entry doesn't exist in the regular document
            for (EventDetail eDetail : evtDetails) {
                String detailName = eDetail.getName();
                if (entry.getKey().equals(detailName)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                String detailKeyName = DETAIL_INDEX_PREFIX + entry.getKey();
                doc.add(new Field(detailKeyName, Character.toString((char)07), Store.NO, Index.NOT_ANALYZED_NO_NORMS));
            }
        }

        for (EventDetail eDetail : evtDetails) {
            String detailName = eDetail.getName();
            EventDetailItem detailDefn = detailsConfig.get(detailName);

            if (detailDefn != null) {
                String detailKeyName = DETAIL_INDEX_PREFIX + detailDefn.getKey();
                for (String detailValue : eDetail.getValueList()) {
                    switch (detailDefn.getType()) {
                        case STRING:
                            doc.add(new Field(detailKeyName, detailValue, Store.NO, Index.NOT_ANALYZED_NO_NORMS));
                            break;
                        case INTEGER:
                            try {
                                int intValue = Integer.parseInt(detailValue);
                                doc.add(new IntField(detailKeyName, intValue, Store.NO));
                            } catch (Exception e) {
                                logger.warn("Invalid numeric(int) data reported for detail {}: {}", detailName,
                                        detailValue);
                            }
                            break;
                        case FLOAT:
                            try {
                                float floatValue = Float.parseFloat(detailValue);
                                doc.add(new FloatField(detailKeyName, floatValue, Store.NO));
                            } catch (Exception e) {
                                logger.warn("Invalid numeric(float) data reported for detail {}: {}", detailName,
                                        detailValue);
                            }
                            break;
                        case LONG:
                            try {
                                long longValue = Long.parseLong(detailValue);
                                doc.add(new LongField(detailKeyName, longValue, Store.NO));
                            } catch (Exception e) {
                                logger.warn("Invalid numeric(long) data reported for detail {}: {}", detailName,
                                        detailValue);
                            }
                            break;
                        case DOUBLE:
                            try {
                                double doubleValue = Double.parseDouble(detailValue);
                                doc.add(new DoubleField(detailKeyName, doubleValue, Store.NO));
                            } catch (Exception e) {
                                logger.warn("Invalid numeric(double) data reported for detail {}: {}", detailName,
                                        detailValue);
                            }
                            break;
                        case IP_ADDRESS:
                            try {
                                if (!detailValue.isEmpty()) {
                                    final InetAddress addr = IpUtils.parseAddress(detailValue);
                                    createIpAddressFields(doc, detailKeyName, addr);
                                }
                            } catch (Exception e) {
                                logger.warn("Invalid IP address data reported for detail {}: {}", detailName,
                                        detailValue);
                            }
                            break;
                        case PATH:
                            createPathFields(doc, detailKeyName, detailValue);
                            break;
                        default:
                            logger.warn("Configured detail {} uses unknown data type: {}, skipping", detailName, detailDefn.getType());
                            break;
                    }
                }
            }
        }
        return doc;
    }

    private static void createPathFields(Document doc, String detailKeyName, String detailValue) {
        String lowerCaseDetailValue = detailValue.toLowerCase();
        doc.add(new TextField(detailKeyName, new PathTokenizer(new StringReader(lowerCaseDetailValue))));
        // Store with a trailing slash
        doc.add(new Field(detailKeyName + SORT_SUFFIX, lowerCaseDetailValue + "/", Store.NO,
                Index.NOT_ANALYZED_NO_NORMS));
    }

    private static void createIpAddressFields(Document doc, String detailKeyName, InetAddress value) {
        final String typeVal = (value instanceof Inet6Address) ? IP_ADDRESS_TYPE_6 : IP_ADDRESS_TYPE_4;
        doc.add(new Field(detailKeyName + IP_ADDRESS_TYPE_SUFFIX, typeVal, Store.NO, Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new Field(detailKeyName + SORT_SUFFIX, IpUtils.canonicalIpAddress(value), Store.NO,
                Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new Field(detailKeyName, new IpTokenizer(new StringReader(value.getHostAddress()))));
    }

    public static EventSummary toEventSummary(Document item) throws ZepException {
        final EventSummary summary;
        BytesRef protobufBytesRef = item.getBinaryValue(FIELD_PROTOBUF);
        if (protobufBytesRef != null) {
            summary = uncompressProtobuf(protobufBytesRef.bytes);
        }
        else {
            // Only other possible fields stored on index.
            final String uuid = item.get(FIELD_UUID);
            EventSummary.Builder summaryBuilder = EventSummary.newBuilder();
            if (uuid != null) {
                summaryBuilder.setUuid(uuid);
            }
            summary = summaryBuilder.build();
        }
        return summary;
    }
}
