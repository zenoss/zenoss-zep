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

package org.zenoss.zep.index.impl;
import java.util.Date;
import java.util.List;

import org.apache.solr.client.solrj.beans.Field;

public class SolrEventIndexItem {
    public static final String FIELD_UUID = "uuid";
    public static final String FIELD_STATUS = "status";
    public static final String FIELD_COUNT = "count";
    public static final String FIELD_LAST_SEEN_TIME = "last_seen_time";
    public static final String FIELD_FIRST_SEEN_TIME = "first_seen_time";
    public static final String FIELD_STATUS_CHANGE_TIME = "status_change_time";
    public static final String FIELD_EVENT_ACTOR_ELEMENT_TYPE_ID = "event_actor_element_type_id";
    public static final String FIELD_EVENT_ACTOR_ELEMENT_UUID = "event_actor_element_uuid";
    public static final String FIELD_EVENT_ACTOR_ELEMENT_IDENTIFIER = "event_actor_element_identifier";
    public static final String FIELD_EVENT_ACTOR_ELEMENT_IDENTIFIER_SORT = "event_actor_element_identifier_sort";
    public static final String FIELD_EVENT_ACTOR_ELEMENT_SUB_TYPE_ID = "event_actor_element_sub_type_id";
    public static final String FIELD_EVENT_ACTOR_ELEMENT_SUB_UUID = "event_actor_element_sub_uuid";
    public static final String FIELD_EVENT_ACTOR_ELEMENT_SUB_IDENTIFIER = "event_actor_element_sub_identifier";
    public static final String FIELD_EVENT_ACTOR_ELEMENT_SUB_IDENTIFIER_SORT = "event_actor_element_sub_identifier_sort";
    public static final String FIELD_EVENT_UUID = "event_uuid";
    public static final String FIELD_EVENT_SUMMARY = "event_summary";
    public static final String FIELD_EVENT_SUMMARY_SORT = "event_summary_sort";
    public static final String FIELD_EVENT_SEVERITY = "event_severity";
    public static final String FIELD_EVENT_EVENT_CLASS = "event_event_class";
    public static final String FIELD_EVENT_EVENT_CLASS_INDEX = "event_event_class_index";
    public static final String FIELD_TAGS = "tag";
    public static final String FIELD_TAG_JSON = "tag_json";
    public static final String FIELD_UPDATE_TIME = "update_time";

    @Field
    String uuid;

    @Field
    Integer status;

    @Field
    Integer count;
    
    @Field
    Date last_seen_time;

    @Field
    Date first_seen_time;

    @Field
    Date status_change_time;
    
    @Field
    String event_actor_element_type_id;

    @Field
    String event_actor_element_uuid;

    @Field
    String event_actor_element_identifier;

    @Field
    String event_actor_element_sub_type_id;

    @Field
    String event_actor_element_sub_uuid;

    @Field
    String event_actor_element_sub_identifier;

    @Field
    String event_uuid;

    @Field
    String event_summary;

    @Field
    Integer event_severity;

    @Field
    String event_event_class;

    @Field
    String event_event_class_index;

    @Field(FIELD_TAGS)
    List<String> tags;

    @Field
    String tag_json;
    
    @Field
    Date update_time;
}

