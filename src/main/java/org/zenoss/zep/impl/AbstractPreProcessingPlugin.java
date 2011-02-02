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
package org.zenoss.zep.impl;

import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.zep.EventContext;
import org.zenoss.zep.EventPreProcessingPlugin;
import org.zenoss.zep.ZepException;

/**
 * Abstract post-processing plug-in which implements all methods but
 * {@link EventPreProcessingPlugin#processEvent(Event, EventContext)}.
 */
public abstract class AbstractPreProcessingPlugin extends AbstractPlugin
        implements EventPreProcessingPlugin {

    @Override
    public abstract Event processEvent(Event event, EventContext ctx)
            throws ZepException;

}
