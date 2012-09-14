-- Copyright (C) 2012, Zenoss Inc.  All Rights Reserved.

--
-- Alters index definitions to provide better index usage in key workflows.
--

DROP INDEX IF EXISTS event_summary_status_id_idx;
DROP INDEX IF EXISTS event_summary_clear_fingerprint_hash_idx;
DROP INDEX IF EXISTS event_summary_severity_id_idx;
DROP INDEX IF EXISTS event_summary_last_seen_idx;

DROP INDEX IF EXISTS event_summary_clear_idx;
CREATE INDEX event_summary_clear_idx ON event_summary(clear_fingerprint_hash, status_id, last_seen);

DROP INDEX IF EXISTS event_summary_age_idx;
CREATE INDEX event_summary_age_idx ON event_summary(severity_id, status_id, last_seen);

DROP INDEX IF EXISTS event_summary_archive_idx;
CREATE INDEX event_summary_archive_idx ON event_summary(status_id, last_seen);

INSERT INTO schema_version (version, installed_time) VALUES(2, NOW());
