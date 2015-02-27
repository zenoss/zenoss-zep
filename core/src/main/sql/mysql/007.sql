-- Copyright (C) 2015, Zenoss Inc.  All Rights Reserved.

--
-- Adds closed_status column and improves related composite indexes.
--

ALTER TABLE event_summary ADD COLUMN closed_status TINYINT(1) NOT NULL DEFAULT 0;
UPDATE event_summary SET closed_status = 1 WHERE status_id IN (3,4,6);
ALTER TABLE event_summary ALTER COLUMN closed_status DROP DEFAULT;

DROP PROCEDURE IF EXISTS drop_index_if_exists;
DELIMITER $$
CREATE PROCEDURE drop_index_if_exists(IN tname VARCHAR(64), IN idx_name VARCHAR(64))
BEGIN
  IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema = DATABASE()
            AND UPPER(table_name) = tname AND UPPER(index_name) = idx_name)
  THEN
    SET @drop_sql = CONCAT('DROP INDEX ',idx_name,' ON ',tname,';');
    PREPARE stmt FROM @drop_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  END IF;
END
$$
DELIMITER ;

-- These are no longer useful.
CALL drop_index_if_exists('event_summary','event_summary_age_idx');
CALL drop_index_if_exists('event_summary','event_summary_archive_idx');

-- A few installs had these indexes created manually.
CALL drop_index_if_exists('event_summary','event_summary_last_seen');
CALL drop_index_if_exists('event_summary','event_summary_last_seen_idx');

CALL drop_index_if_exists('event_summary','event_summary_closed_last_seen_idx');
CREATE INDEX event_summary_closed_last_seen_idx ON event_summary(closed_status, last_seen);

CALL drop_index_if_exists('event_summary','event_summary_closed_uuid_idx');
CREATE INDEX event_summary_closed_uuid_idx ON event_summary(closed_status, uuid);

CALL drop_index_if_exists('event_summary','event_summary_clear_idx');
CREATE INDEX event_summary_clear_idx ON event_summary(closed_status, clear_fingerprint_hash, last_seen);

DROP PROCEDURE IF EXISTS drop_index_if_exists;

INSERT INTO schema_version (version, installed_time) VALUES(7, NOW());
