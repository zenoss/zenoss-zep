-- This program is part of Zenoss Core, an open source monitoring platform.
-- Copyright (C) 2010, Zenoss Inc.
--
-- This program is free software; you can redistribute it and/or modify it
-- under the terms of the GNU General Public License version 2 as published by
-- the Free Software Foundation.
--
-- For complete information please visit: http://www.zenoss.com/oss/

SET storage_engine=InnoDB;

CREATE TABLE `event_class`
(
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(128) NOT NULL UNIQUE,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `element_event_summary`
(
    `element_uuid` BINARY(16) NOT NULL,
    `severity_id` TINYINT NOT NULL,
    `event_count` INTEGER NOT NULL,
    PRIMARY KEY (`element_uuid`,`severity_id`)
) ENGINE=InnoDB CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `event_class_key`
(
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(128) NOT NULL UNIQUE COMMENT 'Free-form text field that is used as the first step in mapping an unknown event into an event class.',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `event_key`
(
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(128) NOT NULL UNIQUE COMMENT 'Free-form text field that allows another specificity key to be used to drive the de-duplication and auto-clearing correlation process.',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `monitor`
(
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(128) NOT NULL UNIQUE,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `agent`
(
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64) NOT NULL UNIQUE,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `event_group`
(
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64) NOT NULL UNIQUE COMMENT 'Free-form text field that can be used to group similar types of events. This is primarily an extension point for customization. Currently not used in a standard system.',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `index_metadata`
(
    `zep_instance` BINARY(16) NOT NULL,
    `index_name` VARCHAR(32) NOT NULL,
    `index_version` INTEGER NOT NULL COMMENT 'Version number of index. Used to determine when it should be rebuilt.',
    `index_version_hash` BINARY(20) COMMENT 'Optional SHA-1 hash of index configuration. Used as secondary factor to determine if it should be rebuilt.',
    `last_index_time` BIGINT NOT NULL COMMENT 'Last update_time that was indexed.',
    `last_commit_time` BIGINT COMMENT 'Last update_time when the index was committed.',
    PRIMARY KEY (`zep_instance`, `index_name`)
) ENGINE=InnoDB CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `event_summary`
(
    `uuid` BINARY(16) NOT NULL,
    `fingerprint_hash` BINARY(20) NOT NULL COMMENT 'SHA-1 hash of the fingerprint.',
    `fingerprint` VARCHAR(255) NOT NULL COMMENT 'Dynamically generated fingerprint that allows the system to perform de-duplication on repeating events that share similar characteristics.',
    `status_id` TINYINT NOT NULL,
    `event_group_id` INTEGER COMMENT 'Can be used to group similar types of events. This is primarily an extension point for customization.',
    `event_class_id` INTEGER NOT NULL,
    `event_class_key_id` INTEGER COMMENT 'Used as the first step in mapping an unknown event into an event class.',
    `event_class_mapping_uuid` BINARY(16) COMMENT 'If this event was matched by one of the configured event class mappings, contains the UUID of that mapping rule.',
    `event_key_id` INTEGER,
    `severity_id` TINYINT NOT NULL,
    `element_uuid` BINARY(16),
    `element_type_id` TINYINT,
    `element_identifier` VARCHAR(255),
    `element_sub_uuid` BINARY(16),
    `element_sub_type_id` TINYINT,
    `element_sub_identifier` VARCHAR(255),
    `update_time` BIGINT NOT NULL COMMENT 'Last time any modification was made to the event. Used to determine whether event should be re-indexed.',
    `first_seen` BIGINT NOT NULL COMMENT 'UTC Time. First time that the event occurred.',
    `status_change` BIGINT NOT NULL COMMENT 'Last time that the event status changed.',
    `last_seen` BIGINT NOT NULL COMMENT 'UTC time. Most recent time that the event occurred.',
    `event_count` INTEGER NOT NULL COMMENT 'Number of occurrences of the event.',
    `monitor_id` INTEGER COMMENT 'In a distributed setup, contains the name of the collector from which the event originated.',
    `agent_id` INTEGER COMMENT 'Typically the name of the daemon that generated the event. For example, an SNMP threshold event will have zenperfsnmp as its agent.',
    `syslog_facility` INTEGER COMMENT 'The syslog facility.',
    `syslog_priority` TINYINT COMMENT 'The syslog priority.',
    `nt_event_code` INTEGER COMMENT 'The Windows NT Event Code.',
    `acknowledged_by_user_uuid` BINARY(16) COMMENT 'UUID of the user who acknowledged this event.',
    `acknowledged_by_user_name` VARCHAR(32) COMMENT 'Name of the user who acknowledged this event.',
    `clear_fingerprint_hash` BINARY(20) COMMENT 'Hash of clear fingerprint used for clearing events.',
    `cleared_by_event_uuid` BINARY(16) COMMENT 'The UUID of the event that cleared this event (for events with status == CLEARED).',
    `summary` VARCHAR(255) NOT NULL DEFAULT '',
    `message` VARCHAR(4096) NOT NULL DEFAULT '',
    `details_json` MEDIUMTEXT COMMENT 'JSON encoded event details.',
    `tags_json` MEDIUMTEXT COMMENT 'JSON encoded event tags.',
    `notes_json` MEDIUMTEXT COMMENT 'JSON encoded event notes (formerly log).',
    PRIMARY KEY (uuid),
    UNIQUE KEY (fingerprint_hash),
    FOREIGN KEY (`event_group_id`) REFERENCES `event_group` (`id`),
    FOREIGN KEY (`event_class_id`) REFERENCES `event_class` (`id`),
    FOREIGN KEY (`event_class_key_id`) REFERENCES `event_class_key` (`id`),
    FOREIGN KEY (`event_key_id`) REFERENCES `event_key` (`id`),
    FOREIGN KEY (`monitor_id`) REFERENCES `monitor` (`id`),
    FOREIGN KEY (`agent_id`) REFERENCES `agent` (`id`),
    INDEX (`status_id`,`last_seen`),
    INDEX (`clear_fingerprint_hash`,`last_seen`),
    INDEX (`severity_id`,`last_seen`),
    INDEX (`update_time`),
    INDEX (`element_uuid`,`element_type_id`,`element_identifier`),
    INDEX (`element_sub_uuid`,`element_sub_type_id`,`element_sub_identifier`)
) ENGINE=InnoDB CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `event_archive`
(
    `uuid` BINARY(16) NOT NULL,
    `fingerprint_hash` BINARY(20) NOT NULL COMMENT 'SHA-1 hash of the fingerprint.',
    `fingerprint` VARCHAR(255) NOT NULL COMMENT 'Dynamically generated fingerprint that allows the system to perform de-duplication on repeating events that share similar characteristics.',
    `status_id` TINYINT NOT NULL,
    `event_group_id` INTEGER COMMENT 'Can be used to group similar types of events. This is primarily an extension point for customization.',
    `event_class_id` INTEGER NOT NULL,
    `event_class_key_id` INTEGER COMMENT 'Used as the first step in mapping an unknown event into an event class.',
    `event_class_mapping_uuid` BINARY(16) COMMENT 'If this event was matched by one of the configured event class mappings, contains the UUID of that mapping rule.',
    `event_key_id` INTEGER,
    `severity_id` TINYINT NOT NULL,
    `element_uuid` BINARY(16),
    `element_type_id` TINYINT,
    `element_identifier` VARCHAR(255),
    `element_sub_uuid` BINARY(16),
    `element_sub_type_id` TINYINT,
    `element_sub_identifier` VARCHAR(255),
    `update_time` BIGINT NOT NULL COMMENT 'Last time any modification was made to the event. Used to determine whether event should be re-indexed.',
    `first_seen` BIGINT NOT NULL COMMENT 'UTC Time. First time that the event occurred.',
    `status_change` BIGINT NOT NULL COMMENT 'Last time that the event status changed.',
    `last_seen` BIGINT NOT NULL COMMENT 'UTC time. Most recent time that the event occurred.',
    `event_count` INTEGER NOT NULL COMMENT 'Number of occurrences of the event.',
    `monitor_id` INTEGER COMMENT 'In a distributed setup, contains the name of the collector from which the event originated.',
    `agent_id` INTEGER COMMENT 'Typically the name of the daemon that generated the event. For example, an SNMP threshold event will have zenperfsnmp as its agent.',
    `syslog_facility` INTEGER COMMENT 'The syslog facility.',
    `syslog_priority` TINYINT COMMENT 'The syslog priority.',
    `nt_event_code` INTEGER COMMENT 'The Windows NT Event Code.',
    `acknowledged_by_user_uuid` BINARY(16) COMMENT 'UUID of the user who acknowledged this event.',
    `acknowledged_by_user_name` VARCHAR(32) COMMENT 'Name of the user who acknowledged this event.',
    `clear_fingerprint_hash` BINARY(20) COMMENT 'Hash of clear fingerprint used for clearing events.',
    `cleared_by_event_uuid` BINARY(16) COMMENT 'The UUID of the event that cleared this event (for events with status == CLEARED).',
    `summary` VARCHAR(255) NOT NULL DEFAULT '',
    `message` VARCHAR(4096) NOT NULL DEFAULT '',
    `details_json` MEDIUMTEXT COMMENT 'JSON encoded event details.',
    `tags_json` MEDIUMTEXT COMMENT 'JSON encoded event tags.',
    `notes_json` MEDIUMTEXT COMMENT 'JSON encoded event notes (formerly log).',
    INDEX (`uuid`),
    INDEX (`update_time`)
) ENGINE=InnoDB CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `event`
(
    `uuid` BINARY(16) NOT NULL,
    `summary_uuid` BINARY(16) NOT NULL COMMENT 'UUID of the event summary this occurrence was de-duplicated into.',
    `fingerprint` VARCHAR(255) NOT NULL COMMENT 'Dynamically generated fingerprint that allows the system to perform de-duplication on repeating events that share similar characteristics.',
    `event_group_id` INTEGER COMMENT 'Can be used to group similar types of events. This is primarily an extension point for customization.',
    `event_class_id` INTEGER NOT NULL,
    `event_class_key_id` INTEGER COMMENT 'Used as the first step in mapping an unknown event into an event class.',
    `event_class_mapping_uuid` BINARY(16) COMMENT 'If this event was matched by one of the configured event class mappings, contains the UUID of that mapping rule.',
    `event_key_id` INTEGER,
    `severity_id` TINYINT NOT NULL,
    `element_uuid` BINARY(16),
    `element_type_id` TINYINT,
    `element_identifier` VARCHAR(255),
    `element_sub_uuid` BINARY(16),
    `element_sub_type_id` TINYINT,
    `element_sub_identifier` VARCHAR(255),
    `created` BIGINT NOT NULL COMMENT 'UTC Time. Time that the event occurred.',
    `monitor_id` INTEGER COMMENT 'In a distributed setup, contains the name of the collector from which the event originated.',
    `agent_id` INTEGER COMMENT 'Typically the name of the daemon that generated the event. For example, an SNMP threshold event will have zenperfsnmp as its agent.',
    `syslog_facility` INTEGER COMMENT 'The syslog facility.',
    `syslog_priority` TINYINT COMMENT 'The syslog priority.',
    `nt_event_code` INTEGER COMMENT 'The Windows NT Event Code.',
    `summary` VARCHAR(255) NOT NULL DEFAULT '',
    `message` VARCHAR(4096) NOT NULL DEFAULT '',
    `details_json` MEDIUMTEXT COMMENT 'JSON encoded event details.',
    `tags_json` MEDIUMTEXT COMMENT 'JSON encoded event tags.',
    INDEX (`uuid`),
    INDEX (`summary_uuid`)
) ENGINE=InnoDB CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `config`
(
    `config_name` VARCHAR(64) NOT NULL,
    `config_value` VARCHAR(255) NOT NULL,
    PRIMARY KEY(config_name)
) ENGINE=InnoDB COMMENT='ZEP configuration data.' CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE event_trigger
(
    uuid BINARY(16) NOT NULL,
    name VARCHAR(255),
    enabled TINYINT NOT NULL,
    rule_api_version TINYINT NOT NULL,
    rule_type_id TINYINT NOT NULL,
    rule_source VARCHAR(8192) NOT NULL,
    PRIMARY KEY (uuid)
) ENGINE = InnoDB CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE event_trigger_subscription
(
    uuid BINARY(16) NOT NULL,
    event_trigger_uuid BINARY(16) NOT NULL,
    subscriber_uuid BINARY(16) NOT NULL,
    delay_seconds INTEGER NOT NULL DEFAULT 0,
    repeat_seconds INTEGER NOT NULL DEFAULT 0,
    send_initial_occurrence TINYINT NOT NULL,
    PRIMARY KEY (uuid),
    FOREIGN KEY (`event_trigger_uuid`) REFERENCES `event_trigger` (`uuid`) ON DELETE CASCADE,
    UNIQUE INDEX (`event_trigger_uuid`,`subscriber_uuid`)
) ENGINE = InnoDB CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `event_trigger_signal_spool`
(
    `uuid` BINARY(16) NOT NULL,
    `event_trigger_subscription_uuid` BINARY(16) NOT NULL,
    `event_summary_uuid` BINARY(16) NOT NULL,
    `flush_time` BIGINT DEFAULT 0 NOT NULL COMMENT 'A signal will be sent when the flush_time is reached',
    `created` BIGINT NOT NULL,
    `event_count` INTEGER DEFAULT 1 NOT NULL COMMENT 'The number of times the event occurred while the signal was in the spool.',
    PRIMARY KEY (`uuid`),
    FOREIGN KEY (`event_trigger_subscription_uuid`) REFERENCES `event_trigger_subscription` (`uuid`) ON DELETE CASCADE,
    FOREIGN KEY (`event_summary_uuid`) REFERENCES `event_summary` (`uuid`) ON DELETE CASCADE,
    UNIQUE INDEX (`event_trigger_subscription_uuid`, `event_summary_uuid`),
    INDEX (`flush_time`),
    INDEX (`created`)
) ENGINE=InnoDB COMMENT='Spool for event flapping.' CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `event_detail_index_config`
(
    `detail_item_name` VARCHAR(255) NOT NULL COMMENT 'EventDetailItem.name',
    `proto_json` MEDIUMTEXT NOT NULL COMMENT 'JSON serialized EventDetailItem',
    PRIMARY KEY (`detail_item_name`)
) ENGINE=InnoDB COMMENT='Event detail index configuration.' CHARACTER SET=utf8 COLLATE=utf8_general_ci;
