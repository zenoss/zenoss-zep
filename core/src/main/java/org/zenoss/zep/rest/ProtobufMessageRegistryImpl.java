/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2010, Zenoss Inc.
 * 
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 * 
 * For complete information please visit: http://www.zenoss.com/oss/
 */
package org.zenoss.zep.rest;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import org.zenoss.protobufs.rest.ProtobufMessageRegistry;
import org.zenoss.protobufs.zep.Zep.DaemonHeartbeatSet;
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

    private final ExtensionRegistry registry = ExtensionRegistry.newInstance();

    public ProtobufMessageRegistryImpl() {
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
                DaemonHeartbeatSet.getDefaultInstance());
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
