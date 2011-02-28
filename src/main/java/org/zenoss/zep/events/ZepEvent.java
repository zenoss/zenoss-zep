/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2011, Zenoss Inc.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 *
 * For complete information please visit: http://www.zenoss.com/oss/
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
