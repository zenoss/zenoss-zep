-- Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.

SET storage_engine=InnoDB;

ALTER TABLE `event_trigger_signal_spool` ADD COLUMN `sent_signal` TINYINT DEFAULT 0 NOT NULL;

-- Set initial state on sent_signal to true for existing spool entries
UPDATE `event_trigger_signal_spool` SET sent_signal=1;

INSERT INTO `schema_version` (`version`, `installed_time`) VALUES(2, NOW());

