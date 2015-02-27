--
-- Adds closed_status column and improves related composite indexes.
--

DROP INDEX IF EXISTS event_summary_age_idx;
DROP INDEX IF EXISTS event_summary_archive_idx;

ALTER TABLE event_summary ADD COLUMN closed_status BOOLEAN NOT NULL DEFAULT FALSE;
UPDATE event_summary SET closed_status = TRUE WHERE status_id IN (3,4,6);
ALTER TABLE event_summary ALTER COLUMN closed_status DROP DEFAULT;

DROP INDEX IF EXISTS event_summary_closed_last_seen_idx;
CREATE INDEX event_summary_closed_last_seen_idx ON event_summary(closed_status, last_seen);

DROP INDEX IF EXISTS event_summary_closed_uuid_idx;
CREATE INDEX event_summary_closed_uuid_idx ON event_summary(closed_status, uuid);

DROP INDEX IF EXISTS event_summary_clear_idx;
CREATE INDEX event_summary_clear_idx ON event_summary(closed_status, clear_fingerprint_hash, last_seen);

INSERT INTO schema_version (version, installed_time) VALUES(3, NOW());
