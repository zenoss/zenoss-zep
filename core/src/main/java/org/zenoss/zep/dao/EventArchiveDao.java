/*
 * Copyright (C) 2010, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.dao;

/**
 * DAO which provides an interface to the event archive.
 */
public interface EventArchiveDao extends EventSummaryBaseDao, Partitionable, Purgable {

}
