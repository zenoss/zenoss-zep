/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */

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
