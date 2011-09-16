/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.rest;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import org.zenoss.protobufs.rest.ProtobufMessageRegistry;
import org.zenoss.protobufs.zep.Zep.DaemonHeartbeatSet;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventDetailItem;
import org.zenoss.protobufs.zep.Zep.EventDetailItemSet;
import org.zenoss.protobufs.zep.Zep.EventDetailSet;
import org.zenoss.protobufs.zep.Zep.EventFilter;
import org.zenoss.protobufs.zep.Zep.EventNote;
import org.zenoss.protobufs.zep.Zep.EventQuery;
import org.zenoss.protobufs.zep.Zep.EventSummaryRequest;
import org.zenoss.protobufs.zep.Zep.EventSummaryUpdate;
import org.zenoss.protobufs.zep.Zep.EventSummaryUpdateRequest;
import org.zenoss.protobufs.zep.Zep.EventSummaryUpdateResponse;
import org.zenoss.protobufs.zep.Zep.EventTagSeveritiesSet;
import org.zenoss.protobufs.zep.Zep.EventTrigger;
import org.zenoss.protobufs.zep.Zep.EventTriggerSubscriptionSet;
import org.zenoss.protobufs.zep.Zep.ZepConfig;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProtobufMessageRegistryImpl implements ProtobufMessageRegistry {

    private final Map<String, Message> messageByFullName = new HashMap<String, Message>();

    private final ExtensionRegistry registry;

    public ProtobufMessageRegistryImpl(ExtensionRegistry extensionRegistry) {
        this.registry = extensionRegistry;
        List<? extends Message> messages = Arrays.asList(
                EventTrigger.getDefaultInstance(),
                EventNote.getDefaultInstance(),
                EventSummaryRequest.getDefaultInstance(),
                EventSummaryUpdateRequest.getDefaultInstance(),
                EventSummaryUpdateResponse.getDefaultInstance(),
                EventSummaryUpdate.getDefaultInstance(),
                EventTriggerSubscriptionSet.getDefaultInstance(),
                EventTagSeveritiesSet.getDefaultInstance(),
                EventQuery.getDefaultInstance(),
                EventDetailSet.getDefaultInstance(),
                EventDetailItemSet.getDefaultInstance(),
                EventDetailItem.getDefaultInstance(),
                ZepConfig.getDefaultInstance(),
                EventFilter.getDefaultInstance(),
                DaemonHeartbeatSet.getDefaultInstance(),
                Event.getDefaultInstance());
        for (Message message : messages) {
            messageByFullName.put(message.getDescriptorForType().getFullName(),
                    message);
        }
    }

    @Override
    public Message getMessageByFullName(String fullName) {
        return messageByFullName.get(fullName);
    }

    @Override
    public ExtensionRegistry getExtensionRegistry() {
        return registry;
    }

}
