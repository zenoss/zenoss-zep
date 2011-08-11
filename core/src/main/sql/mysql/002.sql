-- Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.

SET storage_engine=InnoDB;

ALTER TABLE `event_trigger_signal_spool` ADD COLUMN `sent_signal` TINYINT DEFAULT 0 NOT NULL;

INSERT INTO `schema_version` (`version`, `installed_time`) VALUES(2, NOW());
