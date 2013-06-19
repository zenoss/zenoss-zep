DROP TRIGGER IF EXISTS `event_summary_index_queue_insert`;
DROP TRIGGER IF EXISTS `event_summary_index_queue_update`;
DROP TRIGGER IF EXISTS `event_summary_index_queue_delete`;

INSERT INTO `schema_version` (`version`, `installed_time`) VALUES(6, NOW()); 
