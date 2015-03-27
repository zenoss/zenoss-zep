-- Copyright (C) 2015, Zenoss Inc.  All Rights Reserved.

--
-- no longer using database to queue event ids for indexing
--

DROP TABLE IF EXISTS event_summary_index_queue;

INSERT INTO schema_version (version, installed_time) VALUES(8, NOW());
