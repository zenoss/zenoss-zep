/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.impl;

import org.zenoss.protobufs.zep.Zep.ZepRawEvent;
import org.zenoss.zep.EventContext;

import java.util.HashSet;
import java.util.Set;

public class EventContextImpl implements EventContext {

    private final Set<String> clearClasses = new HashSet<String>();

    public EventContextImpl(ZepRawEvent rawEvent) {
        if (rawEvent == null) {
            throw new NullPointerException();
        }
        this.clearClasses.addAll(rawEvent.getClearEventClassList());
    }

    @Override
    public Set<String> getClearClasses() {
        return this.clearClasses;
    }

    @Override
    public String toString() {
        return String.format("EventContextImpl [clearClasses=%s]", clearClasses);
    }

}
