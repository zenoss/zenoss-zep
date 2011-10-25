-- Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.

CREATE TABLE schema_version
(
    version INTEGER NOT NULL,
    installed_time TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY(version)
);

CREATE TABLE event_class
(
    id SERIAL NOT NULL,
    name VARCHAR(128) NOT NULL UNIQUE,
    PRIMARY KEY (id)
);

CREATE TABLE event_class_key
(
    id SERIAL NOT NULL,
    name VARCHAR(128) NOT NULL UNIQUE,
    PRIMARY KEY (id)
);

CREATE TABLE event_key
(
    id SERIAL NOT NULL,
    name VARCHAR(128) NOT NULL UNIQUE,
    PRIMARY KEY (id)
);

CREATE TABLE monitor
(
    id SERIAL NOT NULL,
    name VARCHAR(128) NOT NULL UNIQUE,
    PRIMARY KEY (id)
);

CREATE TABLE agent
(
    id SERIAL NOT NULL,
    name VARCHAR(64) NOT NULL UNIQUE,
    PRIMARY KEY (id)
);

CREATE TABLE event_group
(
    id SERIAL NOT NULL,
    name VARCHAR(64) NOT NULL UNIQUE,
    PRIMARY KEY (id)
);

CREATE TABLE index_metadata
(
    zep_instance UUID NOT NULL,
    index_name VARCHAR(32) NOT NULL,
    index_version INTEGER NOT NULL,
    index_version_hash BYTEA,
    PRIMARY KEY (zep_instance, index_name)
);

CREATE TABLE event_summary
(
    uuid UUID NOT NULL,
    fingerprint_hash BYTEA NOT NULL,
    fingerprint VARCHAR(255) NOT NULL,
    status_id SMALLINT NOT NULL,
    event_group_id INTEGER,
    event_class_id INTEGER NOT NULL,
    event_class_key_id INTEGER,
    event_class_mapping_uuid UUID,
    event_key_id INTEGER,
    severity_id SMALLINT NOT NULL,
    element_uuid UUID,
    element_type_id SMALLINT,
    element_identifier VARCHAR(255) NOT NULL,
    element_title VARCHAR(255),
    element_sub_uuid UUID,
    element_sub_type_id SMALLINT,
    element_sub_identifier VARCHAR(255),
    element_sub_title VARCHAR(255),
    update_time TIMESTAMP WITH TIME ZONE NOT NULL,
    first_seen TIMESTAMP WITH TIME ZONE NOT NULL,
    status_change TIMESTAMP WITH TIME ZONE NOT NULL,
    last_seen TIMESTAMP WITH TIME ZONE NOT NULL,
    event_count INTEGER NOT NULL,
    monitor_id INTEGER,
    agent_id INTEGER,
    syslog_facility INTEGER,
    syslog_priority SMALLINT,
    nt_event_code INTEGER,
    current_user_uuid UUID,
    current_user_name VARCHAR(32),
    clear_fingerprint_hash BYTEA,
    cleared_by_event_uuid UUID,
    summary VARCHAR(255) NOT NULL DEFAULT '',
    message VARCHAR(4096) NOT NULL DEFAULT '',
    details_json TEXT,
    tags_json TEXT,
    notes_json TEXT,
    audit_json TEXT,
    PRIMARY KEY (uuid),
    UNIQUE (fingerprint_hash),
    FOREIGN KEY (event_group_id) REFERENCES event_group (id),
    FOREIGN KEY (event_class_id) REFERENCES event_class (id),
    FOREIGN KEY (event_class_key_id) REFERENCES event_class_key (id),
    FOREIGN KEY (event_key_id) REFERENCES event_key (id),
    FOREIGN KEY (monitor_id) REFERENCES monitor (id),
    FOREIGN KEY (agent_id) REFERENCES agent (id)
);

CREATE INDEX event_summary_status_id_idx ON event_summary(status_id);
CREATE INDEX event_summary_clear_fingerprint_hash_idx ON event_summary(clear_fingerprint_hash);
CREATE INDEX event_summary_severity_id_idx ON event_summary(severity_id);
CREATE INDEX event_summary_last_seen_idx ON event_summary(last_seen);
CREATE INDEX event_summary_element_idx ON event_summary(element_uuid,element_type_id,element_identifier);
CREATE INDEX event_summary_sub_element_idx ON event_summary(element_sub_uuid,element_sub_type_id,element_sub_identifier);

CREATE TABLE event_summary_index_queue
(
    id BIGSERIAL NOT NULL,
    uuid UUID NOT NULL,
    update_time TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY(id)
);

CREATE TABLE event_archive
(
    uuid UUID NOT NULL,
    fingerprint VARCHAR(255) NOT NULL,
    status_id SMALLINT NOT NULL,
    event_group_id INTEGER,
    event_class_id INTEGER NOT NULL,
    event_class_key_id INTEGER,
    event_class_mapping_uuid UUID,
    event_key_id INTEGER,
    severity_id SMALLINT NOT NULL,
    element_uuid UUID,
    element_type_id SMALLINT,
    element_identifier VARCHAR(255) NOT NULL,
    element_title VARCHAR(255),
    element_sub_uuid UUID,
    element_sub_type_id SMALLINT,
    element_sub_identifier VARCHAR(255),
    element_sub_title VARCHAR(255),
    update_time TIMESTAMP WITH TIME ZONE NOT NULL,
    first_seen TIMESTAMP WITH TIME ZONE NOT NULL,
    status_change TIMESTAMP WITH TIME ZONE NOT NULL,
    last_seen TIMESTAMP WITH TIME ZONE NOT NULL,
    event_count INTEGER NOT NULL,
    monitor_id INTEGER,
    agent_id INTEGER,
    syslog_facility INTEGER,
    syslog_priority SMALLINT,
    nt_event_code INTEGER,
    current_user_uuid UUID,
    current_user_name VARCHAR(32),
    cleared_by_event_uuid UUID,
    summary VARCHAR(255) NOT NULL DEFAULT '',
    message VARCHAR(4096) NOT NULL DEFAULT '',
    details_json TEXT,
    tags_json TEXT,
    notes_json TEXT,
    audit_json TEXT,
    PRIMARY KEY (uuid, last_seen)
);

CREATE TABLE event_archive_index_queue
(
    id BIGSERIAL NOT NULL,
    uuid UUID NOT NULL,
    -- Used for partition pruning in event_archive
    last_seen TIMESTAMP WITH TIME ZONE NOT NULL,
    update_time TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY(id)
);

CREATE TABLE config
(
    config_name VARCHAR(64) NOT NULL,
    config_value VARCHAR(4096) NOT NULL,
    PRIMARY KEY(config_name)
);

CREATE TABLE event_trigger
(
    uuid UUID NOT NULL,
    name VARCHAR(255),
    enabled BOOLEAN NOT NULL,
    rule_api_version SMALLINT NOT NULL,
    rule_type_id SMALLINT NOT NULL,
    rule_source TEXT NOT NULL,
    PRIMARY KEY (uuid)
);

CREATE TABLE event_trigger_subscription
(
    uuid UUID NOT NULL,
    event_trigger_uuid UUID NOT NULL,
    subscriber_uuid UUID NOT NULL,
    delay_seconds INTEGER NOT NULL DEFAULT 0,
    repeat_seconds INTEGER NOT NULL DEFAULT 0,
    send_initial_occurrence BOOLEAN NOT NULL,
    PRIMARY KEY (uuid),
    FOREIGN KEY (event_trigger_uuid) REFERENCES event_trigger (uuid) ON DELETE CASCADE
);

CREATE UNIQUE INDEX event_trigger_subscription_trigger_subscriber_idx ON event_trigger_subscription(event_trigger_uuid,subscriber_uuid);

CREATE TABLE event_trigger_signal_spool
(
    uuid UUID NOT NULL,
    event_trigger_subscription_uuid UUID NOT NULL,
    event_summary_uuid UUID NOT NULL,
    flush_time BIGINT NOT NULL,
    created TIMESTAMP WITH TIME ZONE NOT NULL,
    event_count INTEGER DEFAULT 1 NOT NULL,
    sent_signal BOOLEAN DEFAULT FALSE NOT NULL,
    PRIMARY KEY (uuid),
    FOREIGN KEY (event_trigger_subscription_uuid) REFERENCES event_trigger_subscription (uuid) ON DELETE CASCADE,
    FOREIGN KEY (event_summary_uuid) REFERENCES event_summary (uuid) ON DELETE CASCADE
);

CREATE UNIQUE INDEX event_trigger_signal_spool_subscription_summary_idx ON
  event_trigger_signal_spool(event_trigger_subscription_uuid, event_summary_uuid);
CREATE INDEX event_trigger_signal_spool_flush_time_idx ON event_trigger_signal_spool(flush_time);
CREATE INDEX event_trigger_signal_spool_created_idx ON event_trigger_signal_spool(created);

CREATE TABLE event_detail_index_config
(
    detail_item_name VARCHAR(255) NOT NULL,
    proto_json TEXT NOT NULL,
    PRIMARY KEY (detail_item_name)
);

CREATE TABLE daemon_heartbeat
(
    monitor VARCHAR(255) NOT NULL,
    daemon VARCHAR(255),
    timeout_seconds INTEGER NOT NULL,
    last_time TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (monitor, daemon)
);

CREATE TABLE event_time
(
    summary_uuid UUID NOT NULL,
    processed TIMESTAMP WITH TIME ZONE NOT NULL,
    created TIMESTAMP WITH TIME ZONE NOT NULL,
    first_seen TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX event_time_processed_idx ON event_time(processed);

CREATE LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_queue_summary()
RETURNS TRIGGER AS $$
  BEGIN
    IF (TG_OP = 'DELETE') THEN
      INSERT INTO event_summary_index_queue(uuid,update_time) VALUES(OLD.uuid, OLD.update_time);
    ELSE
      INSERT INTO event_summary_index_queue(uuid,update_time) VALUES(NEW.uuid, NEW.update_time);
    END IF;
    RETURN NULL;
  END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_queue_archive()
RETURNS TRIGGER AS $$
  BEGIN
    IF (TG_OP = 'DELETE') THEN
      INSERT INTO event_archive_index_queue(uuid,last_seen,update_time) VALUES(OLD.uuid, OLD.last_seen, OLD.update_time);
    ELSE
      INSERT INTO event_archive_index_queue(uuid,last_seen,update_time) VALUES(NEW.uuid, NEW.last_seen, NEW.update_time);
    END IF;
    RETURN NULL;
  END
$$ LANGUAGE plpgsql;

CREATE TRIGGER event_summary_index_queue_insert AFTER INSERT OR UPDATE OR DELETE ON event_summary
  FOR EACH ROW EXECUTE PROCEDURE insert_queue_summary();

CREATE TRIGGER event_archive_index_queue_insert AFTER INSERT OR UPDATE OR DELETE ON event_archive
  FOR EACH ROW EXECUTE PROCEDURE insert_queue_archive();

INSERT INTO schema_version (version, installed_time) VALUES(1, NOW());
