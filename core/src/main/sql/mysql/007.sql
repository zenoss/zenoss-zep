-- Copyright (C) 2015, Zenoss Inc.  All Rights Reserved.

--
-- Adds an index for faster aging of events from summary to archive.
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

-- If we don't delete these two, the query planner sometimes chooses to use one
-- of them when it would be better to use event_summary_last_seen_idx.
CALL drop_index_if_exists('event_summary','event_summary_age_idx');
CALL drop_index_if_exists('event_summary','event_summary_archive_idx');

-- At one of our customers, this index was created manually.
CALL drop_index_if_exists('event_summary','event_summary_last_seen');

CALL drop_index_if_exists('event_summary','event_summary_last_seen_idx');
CREATE INDEX event_summary_last_seen_idx ON event_summary(last_seen);

DROP PROCEDURE IF EXISTS drop_index_if_exists;

INSERT INTO schema_version (version, installed_time) VALUES(7, NOW());
