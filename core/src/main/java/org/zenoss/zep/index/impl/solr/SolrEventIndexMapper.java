/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2010, 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/
package org.zenoss.zep.index.impl.solr;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventActor;
import org.zenoss.protobufs.zep.Zep.EventDetail;
import org.zenoss.protobufs.zep.Zep.EventDetailItem;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.EventTag;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.index.impl.BaseEventIndexMapper;
import org.zenoss.zep.index.impl.IndexConstants;
import org.zenoss.zep.utils.IpUtils;

public class SolrEventIndexMapper extends BaseEventIndexMapper {
    public static final String DETAIL_INDEX_PREFIX = "details_";
    public static final String BELL_CHARACTER = Character.toString((char)7);

    private static final Logger logger = LoggerFactory.getLogger(SolrEventIndexMapper.class);

    public static SolrInputDocument fromEventSummary(EventSummary summary, Map<String, EventDetailItem> detailsConfig) throws ZepException {
        final Event event = summary.getOccurrence(0);
        final EventActor actor = event.getActor();

        SolrInputDocument doc = new SolrInputDocument();
        if (!isArchive(event)) // See ZEN-2159
            doc.addField(IndexConstants.FIELD_PROTOBUF, compressProtobuf(summary));

        doc.addField(IndexConstants.FIELD_UUID, summary.getUuid());
        doc.addField(IndexConstants.FIELD_CURRENT_USER_NAME, summary.getCurrentUserName());
        doc.addField(IndexConstants.FIELD_STATUS, summary.getStatus().getNumber());
        doc.addField(IndexConstants.FIELD_COUNT, summary.getCount());
        doc.addField(IndexConstants.FIELD_LAST_SEEN_TIME, summary.getLastSeenTime());
        doc.addField(IndexConstants.FIELD_FIRST_SEEN_TIME, summary.getFirstSeenTime());
        doc.addField(IndexConstants.FIELD_STATUS_CHANGE_TIME, summary.getStatusChangeTime());
        doc.addField(IndexConstants.FIELD_UPDATE_TIME, summary.getUpdateTime());

        doc.addField(IndexConstants.FIELD_FINGERPRINT, event.getFingerprint());
        doc.addField(IndexConstants.FIELD_SUMMARY, event.getSummary());
        doc.addField(IndexConstants.FIELD_SEVERITY, event.getSeverity().getNumber());
        doc.addField(IndexConstants.FIELD_EVENT_CLASS, event.getEventClass());
        doc.addField(IndexConstants.FIELD_AGENT, event.getAgent());
        doc.addField(IndexConstants.FIELD_MONITOR, event.getMonitor());
        doc.addField(IndexConstants.FIELD_EVENT_KEY, event.getEventKey());
        doc.addField(IndexConstants.FIELD_EVENT_CLASS_KEY, event.getEventClassKey());
        doc.addField(IndexConstants.FIELD_EVENT_GROUP, event.getEventGroup());
        doc.addField(IndexConstants.FIELD_MESSAGE, event.getMessage());

        for (EventTag tag : event.getTagsList())
            for (String tagUuid : tag.getUuidList())
                doc.addField(IndexConstants.FIELD_TAGS, tagUuid);

        final String uuid = actor.getElementUuid();
        if (uuid != null && !uuid.isEmpty())
            doc.addField(IndexConstants.FIELD_TAGS, uuid);

        doc.addField(IndexConstants.FIELD_ELEMENT_IDENTIFIER, actor.getElementIdentifier());
        doc.addField(IndexConstants.FIELD_ELEMENT_TITLE, actor.getElementTitle());

        String subUuid = actor.getElementSubUuid();
        if (subUuid != null && !subUuid.isEmpty())
            doc.addField(IndexConstants.FIELD_TAGS, subUuid);

        doc.addField(IndexConstants.FIELD_ELEMENT_SUB_IDENTIFIER, actor.getElementSubIdentifier());
        doc.addField(IndexConstants.FIELD_ELEMENT_SUB_TITLE, actor.getElementSubTitle());

        Set<String> remainingDetails = Sets.newHashSet(detailsConfig.keySet());
        for (EventDetail detail : event.getDetailsList()) {
            final String name = detail.getName();
            final EventDetailItem item = detailsConfig.get(name);
            if (item == null) continue;
            remainingDetails.remove(name);
            EventDetailItem.EventDetailType type = item.getType();
            final String detailKey = DETAIL_INDEX_PREFIX + item.getKey();
            for (String value : detail.getValueList()) {
                try {
                    switch (type) {
                        case STRING:
                            doc.addField(detailKey + "_s", value);
                            doc.addField(detailKey + "_s_sort", value);
                            break;
                        case INTEGER:
                            doc.addField(detailKey + "_i", Integer.parseInt(value));
                            break;
                        case FLOAT:
                            doc.addField(detailKey + "_f", Float.parseFloat(value));
                            break;
                        case LONG:
                            doc.addField(detailKey + "_l", Long.parseLong(value));
                            break;
                        case DOUBLE:
                            doc.addField(detailKey + "_d", Double.parseDouble(value));
                            break;
                        case PATH:
                            doc.addField(detailKey + "_path", value);
                            //NOTE: PATH details are not sortable, because some may be multi-valued.
                            break;
                        case IP_ADDRESS:
                            if (value.isEmpty()) continue;
                            final InetAddress addr = IpUtils.parseAddress(value);
                            final String ipType = (addr instanceof Inet6Address) ?
                                                  IndexConstants.IP_ADDRESS_TYPE_6 :
                                                  IndexConstants.IP_ADDRESS_TYPE_4;
                            doc.addField(detailKey + "_ip", addr.getHostAddress());
                            doc.addField(detailKey + "_ip_type", ipType);
                            doc.addField(detailKey + "_ip_sort", IpUtils.canonicalIpAddress(addr));
                            break;
                        default:
                            throw new IllegalStateException("Unexpected type: " + item.getType());
                    }
                } catch (IllegalArgumentException e) {
                    logger.warn(String.format("Invalid data reported for detail %s(%s): %s", name, type, value));
                } catch (IllegalStateException e) {
                    logger.warn(String.format("Invalid data reported for detail %s(%s): %s", name, type, value));
                } catch (NullPointerException e) {
                    logger.warn(String.format("Invalid data reported for detail %s(%s): %s", name, type, value));
                }
            }
        }
        for (String name : remainingDetails) {
            final String detailKey = DETAIL_INDEX_PREFIX + name;
            final EventDetailItem item = detailsConfig.get(name);
            switch (item.getType()) {
                case STRING:
                    doc.addField(detailKey + "_s", BELL_CHARACTER);
                    break;
                case INTEGER:
                    doc.addField(detailKey + "_i", Integer.MIN_VALUE);
                    break;
                case FLOAT:
                    doc.addField(detailKey + "_f", Integer.MIN_VALUE);
                    break;
                case LONG:
                    doc.addField(detailKey + "_l", Integer.MIN_VALUE);
                    break;
                case DOUBLE:
                    doc.addField(detailKey + "_d", Integer.MIN_VALUE);
                    break;
                case IP_ADDRESS:
                    doc.addField(detailKey + "_ip", BELL_CHARACTER);
                    break;
                case PATH:
                    doc.addField(detailKey + "_path", BELL_CHARACTER);
                    break;
                default:
                    throw new IllegalStateException("Unexpected type: " + item.getType());
            }
        }
        return doc;
    }

    public static boolean isArchive(final Event event) {
        List<String> values = getDetail(event, "is_archive");
        if (values == null) return false;
        for (String value : values)
            if ("true".equals(value)) return true;
        return false;
    }

    public static List<String> getDetail(final Event event, final String detailName) {
        final int count = event.getDetailsCount();
        List<String> values = null;
        for (int i = 0; i < count; i++) {
            final EventDetail detail = event.getDetails(i);
            if (detailName.equals(detail.getName())) {
                if (values == null)
                    values = Lists.newArrayList();
                values.addAll(detail.getValueList());
            }
        }
        return values;
    }

    public static EventSummary toEventSummary(SolrDocument item) throws ZepException {
        final byte[] protobuf = (byte[]) item.getFieldValue(IndexConstants.FIELD_PROTOBUF);
        if (protobuf != null)
            return uncompressProtobuf(protobuf);
        final String uuid = (String) item.getFieldValue(IndexConstants.FIELD_UUID);
        final Long lastSeen = (Long) item.getFieldValue(IndexConstants.FIELD_LAST_SEEN_TIME);
        final EventSummary.Builder builder = EventSummary.newBuilder();
        if (uuid != null)
            builder.setUuid(uuid);
        if (lastSeen != null)
            builder.setLastSeenTime(lastSeen);
        return builder.build();
    }
}
