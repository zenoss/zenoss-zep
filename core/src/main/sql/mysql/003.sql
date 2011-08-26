-- Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.

CREATE TABLE `event_time`
(
    `summary_uuid` BINARY(16) NOT NULL COMMENT 'UUID of the event summary this occurrence was de-duplicated into.',
    `processed` BIGINT NOT NULL COMMENT 'UTC Time. Time the event processed by zep ',
    `created` BIGINT NOT NULL COMMENT 'UTC Time. Time that the event occurred.',
    `first_seen` BIGINT NOT NULL COMMENT 'UTC Time. Time that the event was first seen.',
    INDEX (processed)
) ENGINE=MyISAM CHARACTER SET=utf8 COLLATE=utf8_general_ci;

INSERT INTO `schema_version` (`version`, `installed_time`) VALUES(3, NOW());

