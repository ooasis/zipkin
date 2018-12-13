CREATE TABLE IF NOT EXISTS spans (
             trace_id TEXT,
             name TEXT,
             parent_id TEXT,
             id TEXT,
             kind TEXT,
             timestamp BIGINT,
             duration BIGINT,
             debug BOOLEAN,
             share BOOLEAN,
             local_service_name TEXT,
             local_ipv4 TEXT,
             local_ipv6 TEXT,
             local_port INT,
             remote_service_name TEXT,
             remote_ipv4 TEXT,
             remote_ipv6 TEXT,
             remote_port INT,
             PRIMARY KEY (trace_id, id)
           );

CREATE TABLE IF NOT EXISTS span_annotations (
             trace_id TEXT,
             id TEXT,
             a_timestamp BIGINT,
             a_value TEXT,
             PRIMARY KEY (trace_id, id, a_timestamp)
           );

CREATE TABLE IF NOT EXISTS span_tags (
             trace_id TEXT,
             id TEXT,
             t_key TEXT,
             t_value TEXT,
             PRIMARY KEY (trace_id, id, t_key)
           );
