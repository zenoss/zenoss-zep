/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


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
