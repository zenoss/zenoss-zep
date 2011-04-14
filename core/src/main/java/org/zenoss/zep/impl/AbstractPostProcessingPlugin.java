/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.impl;

import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.zep.EventPostProcessingPlugin;
import org.zenoss.zep.ZepException;

/**
 * Abstract post-processing plug-in which implements all methods but
 * {@link EventPostProcessingPlugin#processEvent(EventSummary)}.
 */
public abstract class AbstractPostProcessingPlugin extends AbstractPlugin
        implements EventPostProcessingPlugin {

    @Override
    public abstract void processEvent(EventSummary event) throws ZepException;

}
