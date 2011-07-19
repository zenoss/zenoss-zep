/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.events;

/**
 * Event fired when the PluginService has been fully initialized. Only then is it safe to start consuming from the
 * AMQP queue listeners that rely on the plug-ins.
 */
public class PluginServiceStartedEvent extends ZepEvent {
    public PluginServiceStartedEvent(Object source) {
        super(source);
    }
}
