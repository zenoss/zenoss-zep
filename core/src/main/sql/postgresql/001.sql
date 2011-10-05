-- Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.

CREATE TABLE schema_version
(
    version INTEGER NOT NULL,
    installed_time TIMESTAMP NOT NULL,
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
    update_time TIMESTAMP NOT NULL,
    first_seen TIMESTAMP NOT NULL,
    status_change TIMESTAMP NOT NULL,
    last_seen TIMESTAMP  NOT NULL,
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
    update_time TIMESTAMP NOT NULL,
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
    update_time TIMESTAMP NOT NULL,
    first_seen TIMESTAMP NOT NULL,
    status_change TIMESTAMP NOT NULL,
    last_seen TIMESTAMP NOT NULL,
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
    last_seen TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
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
    created TIMESTAMP NOT NULL,
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
    last_time TIMESTAMP NOT NULL,
    PRIMARY KEY (monitor, daemon)
);

CREATE TABLE event_time
(
    summary_uuid UUID NOT NULL,
    processed TIMESTAMP NOT NULL,
    created TIMESTAMP NOT NULL,
    first_seen TIMESTAMP NOT NULL
);

CREATE INDEX event_time_processed_idx ON event_time(processed);

CREATE LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION dao_cache_insert(p_table_name TEXT, p_name TEXT)
RETURNS INTEGER AS
$$
DECLARE
  quoted_table_name text := quote_ident(p_table_name);
  quoted_name_value text := quote_literal(p_name);
  id_value integer := NULL;
BEGIN
    LOOP
        EXECUTE 'SELECT id FROM ' || quoted_table_name || ' WHERE name = ' || quoted_name_value INTO id_value;
        IF id_value IS NOT NULL THEN
            RETURN id_value;
        END IF;
        BEGIN
            EXECUTE 'INSERT INTO ' || quoted_table_name || '(name) VALUES(' || quoted_name_value || ')';
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the SELECT again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

-- This contains the subset of fields returned from the event_summary table to perform de-duplication.
CREATE TYPE event_summary_dedup AS (
    event_count INTEGER,
    first_seen TIMESTAMP,
    last_seen TIMESTAMP,
    details_json TEXT,
    status_id SMALLINT,
    uuid UUID
);

-- Attempts to query (and lock) the current event_summary row with the same fingerprint, otherwise attempts creating a
-- new record (with retry logic in case two threads are attempting to create the same row at the same time).
CREATE OR REPLACE FUNCTION event_summary_upsert(p_uuid UUID, p_fingerprint_hash BYTEA, p_fingerprint VARCHAR(255),
                                                p_status_id SMALLINT, p_event_group_id INTEGER,
                                                p_event_class_id INTEGER, p_event_class_key_id INTEGER,
                                                p_event_class_mapping_uuid UUID, p_event_key_id INTEGER,
                                                p_severity_id SMALLINT, p_element_uuid UUID, p_element_type_id SMALLINT,
                                                p_element_identifier VARCHAR(255), p_element_title VARCHAR(255),
                                                p_element_sub_uuid UUID, p_element_sub_type_id SMALLINT,
                                                p_element_sub_identifier VARCHAR(255), p_element_sub_title VARCHAR(255),
                                                p_update_time TIMESTAMP, p_first_seen TIMESTAMP,
                                                p_status_change TIMESTAMP, p_last_seen TIMESTAMP, p_event_count INTEGER,
                                                p_monitor_id INTEGER, p_agent_id INTEGER, p_syslog_facility INTEGER,
                                                p_syslog_priority SMALLINT, p_nt_event_code INTEGER,
                                                p_current_user_uuid UUID, p_current_user_name VARCHAR(32),
                                                p_clear_fingerprint_hash BYTEA, p_cleared_by_event_uuid UUID,
                                                p_summary VARCHAR(255), p_message VARCHAR(4096), p_details_json TEXT,
                                                p_tags_json TEXT, p_notes_json TEXT, p_audit_json TEXT)
RETURNS SETOF event_summary_dedup AS
$$
DECLARE
    r event_summary_dedup;
BEGIN
    LOOP
        SELECT event_count,first_seen,last_seen,details_json,status_id,uuid FROM event_summary
          WHERE fingerprint_hash = p_fingerprint_hash INTO r FOR UPDATE;
        IF found THEN
          RETURN NEXT r;
          EXIT;
        END IF;
        BEGIN
            INSERT INTO event_summary(uuid, fingerprint_hash, fingerprint, status_id, event_group_id, event_class_id,
                                      event_class_key_id, event_class_mapping_uuid, event_key_id, severity_id,
                                      element_uuid, element_type_id, element_identifier, element_title,
                                      element_sub_uuid, element_sub_type_id, element_sub_identifier, element_sub_title,
                                      update_time, first_seen, status_change, last_seen, event_count, monitor_id,
                                      agent_id, syslog_facility, syslog_priority, nt_event_code, current_user_uuid,
                                      current_user_name, clear_fingerprint_hash, cleared_by_event_uuid, summary,
                                      message, details_json, tags_json, notes_json, audit_json)
              VALUES (p_uuid, p_fingerprint_hash, p_fingerprint, p_status_id, p_event_group_id, p_event_class_id,
                      p_event_class_key_id, p_event_class_mapping_uuid, p_event_key_id, p_severity_id, p_element_uuid,
                      p_element_type_id, p_element_identifier, p_element_title, p_element_sub_uuid,
                      p_element_sub_type_id, p_element_sub_identifier, p_element_sub_title, p_update_time, p_first_seen,
                      p_status_change, p_last_seen, p_event_count, p_monitor_id, p_agent_id, p_syslog_facility,
                      p_syslog_priority, p_nt_event_code, p_current_user_uuid, p_current_user_name,
                      p_clear_fingerprint_hash, p_cleared_by_event_uuid, p_summary, p_message, p_details_json,
                      p_tags_json, p_notes_json, p_audit_json);
            EXIT;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the SELECT FOR UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION index_metadata_upsert(p_zep_instance UUID, p_index_name VARCHAR(32), p_index_version INTEGER,
                                                 p_index_version_hash BYTEA)
RETURNS VOID AS
$$
BEGIN
    LOOP
        UPDATE index_metadata SET index_version = p_index_version, index_version_hash = p_index_version_hash
          WHERE zep_instance = p_zep_instance AND index_name = p_index_name;
        IF found THEN
            RETURN;
        END IF;
        BEGIN
            INSERT INTO index_metadata(zep_instance, index_name, index_version, index_version_hash)
              VALUES (p_zep_instance, p_index_name, p_index_version, p_index_version_hash);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION event_detail_index_config_upsert(p_detail_item_name VARCHAR(255), p_proto_json TEXT)
RETURNS VOID AS
$$
BEGIN
    LOOP
        UPDATE event_detail_index_config SET proto_json = p_proto_json WHERE detail_item_name = p_detail_item_name;
        IF found THEN
            RETURN;
        END IF;
        BEGIN
            INSERT INTO event_detail_index_config(detail_item_name,proto_json) VALUES (p_detail_item_name,p_proto_json);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION config_upsert(p_config_name VARCHAR(64), p_config_value VARCHAR(4096))
RETURNS VOID AS
$$
BEGIN
    LOOP
        UPDATE config SET config_value = p_config_value WHERE config_name = p_config_name;
        IF found THEN
            RETURN;
        END IF;
        BEGIN
            INSERT INTO config(config_name,config_value) VALUES (p_config_name,p_config_value);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION daemon_heartbeat_upsert(p_monitor VARCHAR(255), p_daemon VARCHAR(255),
                                                   p_timeout_seconds INTEGER, p_last_time TIMESTAMP)
RETURNS VOID AS
$$
BEGIN
    LOOP
        UPDATE daemon_heartbeat SET timeout_seconds = p_timeout_seconds, last_time = p_last_time
          WHERE monitor = p_monitor AND daemon = p_daemon;
        IF found THEN
            RETURN;
        END IF;
        BEGIN
            INSERT INTO daemon_heartbeat(monitor, daemon, timeout_seconds, last_time)
              VALUES (p_monitor, p_daemon, p_timeout_seconds, p_last_time);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION event_trigger_signal_spool_upsert(p_uuid uuid, p_event_trigger_subscription_uuid uuid,
                                                             p_event_summary_uuid uuid, p_flush_time BIGINT,
                                                             p_created TIMESTAMP, p_event_count INTEGER,
                                                             p_sent_signal BOOLEAN)
RETURNS VOID AS
$$
BEGIN
    LOOP
        UPDATE event_trigger_signal_spool SET event_count = event_count + 1 WHERE uuid = p_uuid;
        IF found THEN
            RETURN;
        END IF;
        BEGIN
            INSERT INTO event_trigger_signal_spool(uuid, event_trigger_subscription_uuid, event_summary_uuid,
                                                   flush_time, created, event_count, sent_signal)
                                                   VALUES (p_uuid, p_event_trigger_subscription_uuid,
                                                   p_event_summary_uuid, p_flush_time, p_created, p_event_count,
                                                   p_sent_signal);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

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
