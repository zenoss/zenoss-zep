/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
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
