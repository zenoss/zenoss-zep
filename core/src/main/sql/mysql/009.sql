-- Copyright (C) 2015, Zenoss Inc.  All Rights Reserved.

--
-- Drop view that references a non-existent table
--

DROP VIEW v_event_summary_index_queue;

INSERT INTO schema_version (version, installed_time) VALUES(9, NOW());
