-- Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.

SET storage_engine=InnoDB;

-- DROP unused columns from event_archive
ALTER TABLE `event_archive` DROP COLUMN `fingerprint_hash`;
ALTER TABLE `event_archive` DROP COLUMN `clear_fingerprint_hash`;

INSERT INTO `schema_version` (`version`, `installed_time`) VALUES(2, NOW());
