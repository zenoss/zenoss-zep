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
