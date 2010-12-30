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
    `name` VARCHAR(128) NOT NULL UNIQUE COMMENT 'Free-form text field (maximum 128 characters) that is used as the first step in mapping an unknown event into an event class.',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `event_key`
(
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(128) NOT NULL UNIQUE COMMENT 'Free-form text field (maximum 128 characters) that allows another specificity key to be used to drive the de-duplication and auto-clearing correlation process.',
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
    `name` VARCHAR(64) NOT NULL UNIQUE COMMENT 'Free-form text field (maximum 64 characters) that can be used to group similar types of events. This is primarily an extension point for customization. Currently not used in a standard system.',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `index_version`
(
    `zep_instance` BINARY(16) NOT NULL,
    `index_name` VARCHAR(32) NOT NULL,
    `last_index_time` BIGINT NOT NULL COMMENT 'Last event_summary.update_time that was indexed.',
    PRIMARY KEY (`zep_instance`, `index_name`)
) ENGINE=InnoDB CHARACTER SET=utf8 COLLATE=utf8_general_ci;
 
CREATE TABLE `event_summary`
(
    `uuid` BINARY(16) NOT NULL,
    `fingerprint_hash` BINARY(20) NOT NULL COMMENT 'SHA-1 hash of the dedupid. Dynamically generated fingerprint that allows the system to perform de-duplication on repeating events that share similar characteristics.',
    `fingerprint` VARCHAR(255) NOT NULL COMMENT 'Human readable dedupid. Dynamically generated fingerprint that allows the system to perform de-duplication on repeating events that share similar characteristics.',
    `status_id` TINYINT NOT NULL,
    `event_group_id` INTEGER COMMENT 'Can be used to group similar types of events. This is primarily an extension point for customization. Currently not used in a standard system.',
    `event_class_id` INTEGER NOT NULL,
    `event_class_key_id` INTEGER COMMENT 'Used as the first step in mapping an unknown event into an event class.',
    `event_class_mapping_uuid` BINARY(16) COMMENT 'If this event was matched by one of the configured event class mappings, contains the name of that mapping rule.',
    `event_key_id` INTEGER,
    `severity_id` TINYINT NOT NULL,
    `element_uuid` BINARY(16),
    `element_type_id` TINYINT,
    `element_identifier` VARCHAR(255) COMMENT 'Identifier used for element.',
    `element_sub_uuid` BINARY(16) COMMENT 'Unique identifier for sub element',
    `element_sub_type_id` TINYINT,
    `element_sub_identifier` VARCHAR(255) COMMENT 'Identifier used for sub element.',
    `update_time` BIGINT NOT NULL COMMENT 'Used to determine whether event has been indexed.',
    `first_seen` BIGINT NOT NULL COMMENT 'UTC Time. First time that the event occurred.',
    `status_change` BIGINT NOT NULL COMMENT 'Last time that any information about the event changed.',
    `last_seen` BIGINT NOT NULL COMMENT 'UTC time. Most recent time that the event occurred.',
    `event_count` INTEGER NOT NULL COMMENT 'Number of occurrences of the event between the firstTime and lastTime.',
    `monitor_id` INTEGER COMMENT 'In a distributed setup, contains the name of the collector from which the event originated.',
    `agent_id` INTEGER COMMENT 'Typically the name of the daemon that generated the event. For example, an SNMP threshold event will have zenperfsnmp as its agent.',
    `syslog_facility` INTEGER COMMENT 'Only present on events coming from syslog. The syslog facility.',
    `syslog_priority` TINYINT COMMENT 'Only present on events coming from syslog. The syslog priority.',
    `nt_event_code` INTEGER COMMENT 'Only present on events coming from Windows event log. The NT Event Code.',
    `acknowledged_by_user_uuid` BINARY(16) COMMENT 'UUID of the user who acknowledged this event.',
    `acknowledged_by_user_name` VARCHAR(32) COMMENT 'User name who acknowledged this event.',
    `clear_fingerprint_hash` BINARY(20) COMMENT 'Hash of clear fingerprint used for clearing events.',
    `cleared_by_event_uuid` BINARY(16) COMMENT 'Only present on events in archive that were cleared. The uuid of the event that cleared this one.',
    `summary` VARCHAR(255) NOT NULL DEFAULT '',
    `message` VARCHAR(4096) NOT NULL DEFAULT '',
    `details_json` MEDIUMTEXT COMMENT 'JSON encoded event details.',
    `tags_json` MEDIUMTEXT COMMENT 'JSON encoded event tags.',
    `notes_json` MEDIUMTEXT COMMENT 'Event notes (formerly log).',
    PRIMARY KEY (uuid),
    UNIQUE KEY (fingerprint_hash),
-- No foreign keys to reduce dead locks (we don't cascade anyway so what's the point?)    
--     FOREIGN KEY (`event_group_id`) REFERENCES `event_group` (`id`),
--     FOREIGN KEY (`event_class_id`) REFERENCES `event_class` (`id`),
--     FOREIGN KEY (`event_class_key_id`) REFERENCES `event_class_key` (`id`),
--     FOREIGN KEY (`event_key_id`) REFERENCES `event_key` (`id`),
--     FOREIGN KEY (`monitor_id`) REFERENCES `monitor` (`id`),
--     FOREIGN KEY (`agent_id`) REFERENCES `agent` (`id`),
    INDEX (`status_id`,`last_seen`),
    INDEX (`clear_fingerprint_hash`,`last_seen`),
    INDEX (`severity_id`,`last_seen`),
    INDEX (`update_time`),
    INDEX (`element_uuid`,`element_type_id`,`element_identifier`)
) ENGINE=InnoDB COMMENT='Contains details about the most recent record.' CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `event_archive`
(
    `uuid` BINARY(16) NOT NULL,
    `fingerprint_hash` BINARY(20) NOT NULL COMMENT 'SHA-1 hash of the dedupid. Dynamically generated fingerprint that allows the system to perform de-duplication on repeating events that share similar characteristics.',
    `fingerprint` VARCHAR(255) NOT NULL COMMENT 'Human readable dedupid. Dynamically generated fingerprint that allows the system to perform de-duplication on repeating events that share similar characteristics.',
    `status_id` TINYINT NOT NULL,
    `event_group_id` INTEGER COMMENT 'Can be used to group similar types of events. This is primarily an extension point for customization. Currently not used in a standard system.',
    `event_class_id` INTEGER NOT NULL,
    `event_class_key_id` INTEGER COMMENT 'Used as the first step in mapping an unknown event into an event class.',
    `event_class_mapping_uuid` BINARY(16) COMMENT 'If this event was matched by one of the configured event class mappings, contains the name of that mapping rule.',
    `event_key_id` INTEGER,
    `severity_id` TINYINT NOT NULL,
    `element_uuid` BINARY(16),
    `element_type_id` TINYINT,
    `element_identifier` VARCHAR(255) COMMENT 'Identifier used for element.',
    `element_sub_uuid` BINARY(16) COMMENT 'Unique identifier for sub element',
    `element_sub_type_id` TINYINT,
    `element_sub_identifier` VARCHAR(255) COMMENT 'Identifier used for sub element.',
    `update_time` BIGINT NOT NULL COMMENT 'Used to determine whether event has been indexed.',
    `first_seen` BIGINT NOT NULL COMMENT 'UTC Time. First time that the event occurred.',
    `status_change` BIGINT NOT NULL COMMENT 'Last time that any information about the event changed.',
    `last_seen` BIGINT NOT NULL COMMENT 'UTC time. Most recent time that the event occurred.',
    `event_count` INTEGER NOT NULL COMMENT 'Number of occurrences of the event between the firstTime and lastTime.',
    `monitor_id` INTEGER COMMENT 'In a distributed setup, contains the name of the collector from which the event originated.',
    `agent_id` INTEGER COMMENT 'Typically the name of the daemon that generated the event. For example, an SNMP threshold event will have zenperfsnmp as its agent.',
    `syslog_facility` INTEGER COMMENT 'Only present on events coming from syslog. The syslog facility.',
    `syslog_priority` TINYINT COMMENT 'Only present on events coming from syslog. The syslog priority.',
    `nt_event_code` INTEGER COMMENT 'Only present on events coming from Windows event log. The NT Event Code.',
    `acknowledged_by_user_uuid` BINARY(16) COMMENT 'UUID of the user who acknowledged this event.',
    `acknowledged_by_user_name` VARCHAR(32) COMMENT 'User name who acknowledged this event.',
    `clear_fingerprint_hash` BINARY(20) COMMENT 'SHA-1 hash of clear fingerprint used for clearing events.',
    `cleared_by_event_uuid` BINARY(16) COMMENT 'Only present on events in archive that were auto-cleared. The uuid of the event that cleared this one.',
    `summary` VARCHAR(255) NOT NULL DEFAULT '',
    `message` VARCHAR(4096) NOT NULL DEFAULT '',
    `details_json` MEDIUMTEXT COMMENT 'JSON encoded event details.',
    `tags_json` MEDIUMTEXT COMMENT 'JSON encoded event tags.',
    `notes_json` MEDIUMTEXT COMMENT 'Event notes (formerly log).',
    INDEX (`uuid`),
    INDEX (`update_time`)
) ENGINE=InnoDB COMMENT='Contains details about archived event summaries.' CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `event`
(
    `uuid` BINARY(16) NOT NULL,
    `fingerprint_hash` BINARY(20) NOT NULL COMMENT 'SHA-1 hash of the dedupid. Dynamically generated fingerprint that allows the system to perform de-duplication on repeating events that share similar characteristics.',
    `fingerprint` VARCHAR(255) NOT NULL COMMENT 'Human readable dedupid. Dynamically generated fingerprint that allows the system to perform de-duplication on repeating events that share similar characteristics.',
    `event_group_id` INTEGER COMMENT 'Can be used to group similar types of events. This is primarily an extension point for customization. Currently not used in a standard system.',
    `event_class_id` INTEGER NOT NULL,
    `event_class_key_id` INTEGER COMMENT 'Used as the first step in mapping an unknown event into an event class.',
    `event_class_mapping_uuid` BINARY(16) COMMENT 'If this event was matched by one of the configured event class mappings, contains the name of that mapping rule.',
    `event_key_id` INTEGER,
    `severity_id` TINYINT NOT NULL,
    `element_uuid` BINARY(16),
    `element_type_id` TINYINT,
    `element_identifier` VARCHAR(255) COMMENT 'Identifier used for element.',
    `element_sub_uuid` BINARY(16) COMMENT 'Unique identifier for sub element',
    `element_sub_type_id` TINYINT,
    `element_sub_identifier` VARCHAR(255) COMMENT 'Identifier used for sub element.',
    `created` BIGINT NOT NULL COMMENT 'UTC Time. Time that the event occurred.',
    `monitor_id` INTEGER COMMENT 'In a distributed setup, contains the name of the collector from which the event originated.',
    `agent_id` INTEGER COMMENT 'Typically the name of the daemon that generated the event. For example, an SNMP threshold event will have zenperfsnmp as its agent.',
    `syslog_facility` INTEGER COMMENT 'Only present on events coming from syslog. The syslog facility.',
    `syslog_priority` TINYINT COMMENT 'Only present on events coming from syslog. The syslog priority.',
    `nt_event_code` INTEGER COMMENT 'Only present on events coming from Windows event log. The NT EventCode.',
    `summary` VARCHAR(255) NOT NULL DEFAULT '',
    `message` VARCHAR(4096) NOT NULL DEFAULT '',
    `details_json` MEDIUMTEXT COMMENT 'JSON encoded event details.',
    `tags_json` MEDIUMTEXT COMMENT 'JSON encoded event tags.',
    INDEX (`uuid`),
    INDEX (`fingerprint_hash`)
) ENGINE=InnoDB COMMENT='Every occurrence of an event goes here.' CHARACTER SET=utf8 COLLATE=utf8_general_ci;

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
    PRIMARY KEY (uuid),
    FOREIGN KEY (`event_trigger_uuid`) REFERENCES `event_trigger` (`uuid`) ON DELETE CASCADE,
    UNIQUE INDEX (`event_trigger_uuid`,`subscriber_uuid`)
) ENGINE = InnoDB CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `event_trigger_signal_spool`
(
    `uuid` BINARY(16) NOT NULL,
    `event_trigger_subscription_uuid` BINARY(16) NOT NULL,
    `event_summary_uuid` BINARY(16) NOT NULL,
    `flush_time` BIGINT DEFAULT 0 NOT NULL COMMENT 'This Signal will be sent when the flush_time is reached',
    `created` BIGINT NOT NULL,
    `event_count` INTEGER DEFAULT 1 NOT NULL COMMENT 'The number of times the event occured while the Signal was in the spool.',
    PRIMARY KEY (`uuid`),
    FOREIGN KEY (`event_trigger_subscription_uuid`) REFERENCES `event_trigger_subscription` (`uuid`) ON DELETE CASCADE,
    FOREIGN KEY (`event_summary_uuid`) REFERENCES `event_summary` (`uuid`) ON DELETE CASCADE,
    UNIQUE INDEX (`event_trigger_subscription_uuid`, `event_summary_uuid`),
    INDEX (`flush_time`),
    INDEX (`created`)
) ENGINE=InnoDB COMMENT='Spool for event flapping.' CHARACTER SET=utf8 COLLATE=utf8_general_ci;

CREATE TABLE `clear_events`
(
    `event_summary_uuid` BINARY(16) NOT NULL,
    `clear_fingerprint_hash` BINARY(20) NOT NULL,
    `last_seen` BIGINT NOT NULL,
    FOREIGN KEY (`event_summary_uuid`) REFERENCES `event_summary` (`uuid`) ON DELETE CASCADE,
    INDEX (`clear_fingerprint_hash`,`last_seen`)
) ENGINE=InnoDB CHARACTER SET=utf8 COLLATE=utf8_general_ci;

