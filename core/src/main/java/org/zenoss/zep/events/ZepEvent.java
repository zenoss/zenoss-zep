/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.events;

import org.springframework.context.ApplicationEvent;

/**
 * Base event class.
 */
public abstract class ZepEvent extends ApplicationEvent {
    protected ZepEvent(Object source) {
        super(source);
    }
}
