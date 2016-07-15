-- Copyright (C) 2015, Zenoss Inc.  All Rights Reserved.

--
-- Drop view that references a non-existent table
--

DROP VIEW v_event_summary_index_queue;

--
-- Make all of the tables that are cached in DaoCacheImpl case sensitive:
--

-- Make event_class table case sensitive
ALTER TABLE event_class CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin;

-- Make event_class_key table case sensitive
ALTER TABLE event_class_key CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin;

-- Make monitor table case sensitive
ALTER TABLE monitor CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin;

-- Make agent table case sensitive
ALTER TABLE agent CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin;

-- Make event_group table case sensitive
ALTER TABLE event_group CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin;

-- Make event_key table case sensitive
ALTER TABLE event_key CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin;


INSERT INTO schema_version (version, installed_time) VALUES(9, NOW());
