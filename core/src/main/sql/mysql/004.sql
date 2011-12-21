-- Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.

--
-- Provides some convenience functions and views for dealing with
-- binary encoded UUIDs, BIGINT timestamps, and binary encoded hashes
-- in the event database tables.
--
-- NOTE:
-- Querying these views should be for debugging only as they do not use
-- indexes on the defined tables and will result in full table scans.
--

DROP FUNCTION IF EXISTS STR_UUID_TO_BINARY;
-- Converts a string UUID (can be NULL) to a binary UUID
CREATE FUNCTION STR_UUID_TO_BINARY (s_uuid CHAR(36))
  RETURNS BINARY(16) DETERMINISTIC NO SQL
  RETURN IF(s_uuid IS NOT NULL,UNHEX(REPLACE(s_uuid, '-', '')),NULL);

DROP FUNCTION IF EXISTS BINARY_SHA1_TO_STR;
-- Converts a binary SHA-1 hash (can be NULL) to a string
CREATE FUNCTION BINARY_SHA1_TO_STR(sha1_hash BINARY(20))
  RETURNS CHAR(40) DETERMINISTIC NO SQL
  RETURN IF(sha1_hash IS NOT NULL,LOWER(HEX(sha1_hash)),NULL);

DROP FUNCTION IF EXISTS UNIX_MS_TO_DATETIME;
-- Converts a timestamp in milliseconds since to epoch to 'YYYY-MM-DD HH:MM:SS.mmm' format
CREATE FUNCTION UNIX_MS_TO_DATETIME(epoch_millis BIGINT)
  RETURNS CHAR(23) DETERMINISTIC NO SQL
  RETURN CONCAT_WS('.', FROM_UNIXTIME(epoch_millis DIV 1000), LPAD(epoch_millis MOD 1000, 3, '0'));

DROP FUNCTION IF EXISTS BINARY_UUID_TO_STR;
DELIMITER |
-- Converts a binary UUID (can be NULL) to a string UUID
CREATE FUNCTION BINARY_UUID_TO_STR (uuid BINARY(16))
  RETURNS CHAR(36) DETERMINISTIC NO SQL
  BEGIN
  DECLARE str_uuid CHAR(32);
  SET str_uuid = IF(uuid IS NOT NULL,LOWER(HEX(uuid)),NULL);
  RETURN IF(uuid IS NOT NULL,CONCAT_WS('-',SUBSTR(str_uuid,1,8),SUBSTR(str_uuid,9,4),SUBSTR(str_uuid,13,4),SUBSTR(str_uuid,17,4),SUBSTR(str_uuid,21,12)),NULL);
  END
|

DELIMITER ;

DROP VIEW IF EXISTS `v_index_metadata`;
CREATE VIEW `v_index_metadata` AS
  SELECT BINARY_UUID_TO_STR(zep_instance) AS zep_instance,
         index_name,
         index_version,
         BINARY_SHA1_TO_STR(index_version_hash) AS index_version_hash
         FROM index_metadata;

DROP VIEW IF EXISTS `v_event_summary`;
CREATE VIEW `v_event_summary` AS
  SELECT BINARY_UUID_TO_STR(uuid) AS uuid,
         BINARY_SHA1_TO_STR(fingerprint_hash) AS fingerprint_hash,
         fingerprint,
         status_id,
         event_group.name AS event_group,
         event_class.name AS event_class,
         event_class_key.name AS event_class_key,
         BINARY_UUID_TO_STR(event_class_mapping_uuid) AS event_class_mapping_uuid,
         event_key.name AS event_key,
         severity_id,
         BINARY_UUID_TO_STR(element_uuid) AS element_uuid,
         element_type_id,
         element_identifier,
         element_title,
         BINARY_UUID_TO_STR(element_sub_uuid) AS element_sub_uuid,
         element_sub_type_id,
         element_sub_identifier,
         element_sub_title,
         UNIX_MS_TO_DATETIME(update_time) AS update_time,
         UNIX_MS_TO_DATETIME(first_seen) AS first_seen,
         UNIX_MS_TO_DATETIME(status_change) AS status_change,
         UNIX_MS_TO_DATETIME(last_seen) AS last_seen,
         event_count,
         monitor.name AS monitor,
         agent.name AS agent,
         syslog_facility,
         syslog_priority,
         nt_event_code,
         BINARY_UUID_TO_STR(current_user_uuid) AS current_user_uuid,
         current_user_name,
         BINARY_SHA1_TO_STR(clear_fingerprint_hash) AS clear_fingerprint_hash,
         BINARY_UUID_TO_STR(cleared_by_event_uuid) AS cleared_by_event_uuid,
         summary,
         message,
         details_json,
         tags_json,
         notes_json,
         audit_json
         FROM event_summary
         LEFT JOIN event_group ON event_summary.event_group_id = event_group.id
         INNER JOIN event_class ON event_summary.event_class_id = event_class.id
         LEFT JOIN event_class_key ON event_summary.event_class_key_id = event_class_key.id
         LEFT JOIN event_key ON event_summary.event_key_id = event_key.id
         LEFT JOIN monitor ON event_summary.monitor_id = monitor.id
         LEFT JOIN agent ON event_summary.agent_id = agent.id;

DROP VIEW IF EXISTS `v_event_summary_index_queue`;
CREATE VIEW `v_event_summary_index_queue` AS
  SELECT id,
         BINARY_UUID_TO_STR(uuid) AS uuid,
         UNIX_MS_TO_DATETIME(update_time) AS update_time
         FROM event_summary_index_queue;

DROP VIEW IF EXISTS `v_event_archive`;
CREATE VIEW `v_event_archive` AS
  SELECT BINARY_UUID_TO_STR(uuid) AS uuid,
         fingerprint,
         status_id,
         event_group.name AS event_group,
         event_class.name AS event_class,
         event_class_key.name AS event_class_key,
         BINARY_UUID_TO_STR(event_class_mapping_uuid) AS event_class_mapping_uuid,
         event_key.name AS event_key,
         severity_id,
         BINARY_UUID_TO_STR(element_uuid) AS element_uuid,
         element_type_id,
         element_identifier,
         element_title,
         BINARY_UUID_TO_STR(element_sub_uuid) AS element_sub_uuid,
         element_sub_type_id,
         element_sub_identifier,
         element_sub_title,
         UNIX_MS_TO_DATETIME(update_time) AS update_time,
         UNIX_MS_TO_DATETIME(first_seen) AS first_seen,
         UNIX_MS_TO_DATETIME(status_change) AS status_change,
         UNIX_MS_TO_DATETIME(last_seen) AS last_seen,
         event_count,
         monitor.name AS monitor,
         agent.name AS agent,
         syslog_facility,
         syslog_priority,
         nt_event_code,
         BINARY_UUID_TO_STR(current_user_uuid) AS current_user_uuid,
         current_user_name,
         BINARY_UUID_TO_STR(cleared_by_event_uuid) AS cleared_by_event_uuid,
         summary,
         message,
         details_json,
         tags_json,
         notes_json,
         audit_json
         FROM event_archive
         LEFT JOIN event_group ON event_archive.event_group_id = event_group.id
         INNER JOIN event_class ON event_archive.event_class_id = event_class.id
         LEFT JOIN event_class_key ON event_archive.event_class_key_id = event_class_key.id
         LEFT JOIN event_key ON event_archive.event_key_id = event_key.id
         LEFT JOIN monitor ON event_archive.monitor_id = monitor.id
         LEFT JOIN agent ON event_archive.agent_id = agent.id;

DROP VIEW IF EXISTS `v_event_archive_index_queue`;
CREATE VIEW `v_event_archive_index_queue` AS
  SELECT id,
         BINARY_UUID_TO_STR(uuid) AS uuid,
         UNIX_MS_TO_DATETIME(last_seen) AS last_seen,
         UNIX_MS_TO_DATETIME(update_time) AS update_time
         FROM event_archive_index_queue;

DROP VIEW IF EXISTS `v_event_trigger`;
CREATE VIEW `v_event_trigger` AS
  SELECT BINARY_UUID_TO_STR(uuid) AS uuid,
         name,
         enabled,
         rule_api_version,
         rule_type_id,
         rule_source FROM event_trigger;

DROP VIEW IF EXISTS `v_event_trigger_subscription`;
CREATE VIEW `v_event_trigger_subscription` AS
  SELECT BINARY_UUID_TO_STR(uuid) AS uuid,
         BINARY_UUID_TO_STR(event_trigger_uuid) AS event_trigger_uuid,
         BINARY_UUID_TO_STR(subscriber_uuid) AS subscriber_uuid,
         delay_seconds,
         repeat_seconds,
         send_initial_occurrence FROM event_trigger_subscription;

DROP VIEW IF EXISTS `v_event_trigger_signal_spool`;
CREATE VIEW `v_event_trigger_signal_spool` AS
  SELECT BINARY_UUID_TO_STR(uuid) AS uuid,
         BINARY_UUID_TO_STR(event_trigger_subscription_uuid) AS event_trigger_subscription_uuid,
         BINARY_UUID_TO_STR(event_summary_uuid) AS event_summary_uuid,
         UNIX_MS_TO_DATETIME(flush_time) AS flush_time,
         UNIX_MS_TO_DATETIME(created) AS created,
         event_count,
         sent_signal FROM event_trigger_signal_spool;

DROP VIEW IF EXISTS `v_daemon_heartbeat`;
CREATE VIEW `v_daemon_heartbeat` AS
  SELECT monitor,
         daemon,
         timeout_seconds,
         UNIX_MS_TO_DATETIME(last_time) AS last_time FROM daemon_heartbeat;

DROP VIEW IF EXISTS `v_event_time`;
CREATE VIEW `v_event_time` AS
  SELECT BINARY_UUID_TO_STR(summary_uuid) AS summary_uuid,
         UNIX_MS_TO_DATETIME(processed) AS processed,
         UNIX_MS_TO_DATETIME(created) AS created,
         UNIX_MS_TO_DATETIME(first_seen) AS first_seen FROM event_time;

INSERT INTO `schema_version` (`version`, `installed_time`) VALUES(4, NOW());
