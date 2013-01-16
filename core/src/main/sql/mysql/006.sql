grant select on mysql.proc to zenoss;
flush privileges;
INSERT INTO schema_version (version, installed_time) VALUES(6, NOW());

