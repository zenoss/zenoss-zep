--
-- Adds an index for faster aging of events from summary to archive.
--

DROP INDEX IF EXISTS event_summary_age_idx;
DROP INDEX IF EXISTS event_summary_archive_idx;

DROP INDEX IF EXISTS event_summary_last_seen;
CREATE INDEX event_summary_last_seen_idx ON event_summary(last_seen);

INSERT INTO schema_version (version, installed_time) VALUES(3, NOW());
