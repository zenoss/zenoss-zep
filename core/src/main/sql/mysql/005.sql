-- Copyright (C) 2012, Zenoss Inc.  All Rights Reserved.

--
-- Alters index definitions to provide better index usage in key workflows.
--

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

CALL drop_index_if_exists('event_summary','status_id');
CALL drop_index_if_exists('event_summary','clear_fingerprint_hash');
CALL drop_index_if_exists('event_summary','severity_id');
CALL drop_index_if_exists('event_summary','last_seen');

CALL drop_index_if_exists('event_summary','event_summary_clear_idx');
CREATE INDEX event_summary_clear_idx ON event_summary(clear_fingerprint_hash, status_id, last_seen);

CALL drop_index_if_exists('event_summary','event_summary_age_idx');
CREATE INDEX event_summary_age_idx ON event_summary(severity_id,status_id,last_seen);

CALL drop_index_if_exists('event_summary','event_summary_archive_idx');
CREATE INDEX event_summary_archive_idx ON event_summary(status_id,last_seen);

DROP PROCEDURE IF EXISTS drop_index_if_exists;

INSERT INTO schema_version (version, installed_time) VALUES(5, NOW());
