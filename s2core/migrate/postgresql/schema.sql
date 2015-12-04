CREATE DATABASE IF NOT EXISTS graph_dev;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO graph;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO graph;



DROP TABLE IF EXISTS services;
CREATE TABLE services (
  id serial PRIMARY KEY,
  service_name varchar(64) NOT NULL,
  access_token varchar(64) NOT NULL,
  cluster varchar(255) NOT NULL,
  hbase_table_name varchar(255) NOT NULL,
  pre_split_size integer NOT NULL default 0,
  hbase_table_ttl integer
);

CREATE UNIQUE INDEX ux_service_name ON services (service_name);
CREATE INDEX idx_access_token ON services (access_token);
CREATE INDEX idx_cluster ON services (cluster);


DROP TABLE IF EXISTS service_columns;
CREATE TABLE service_columns (
  id serial PRIMARY KEY,
  service_id integer NOT NULL,
  column_name varchar(64) NOT NULL,
  column_type varchar(8) NOT NULL,
  schema_version varchar(8) NOT NULL default 'v3'
);

CREATE UNIQUE INDEX ux_service_id_column_name ON service_columns (service_id, column_name);
ALTER TABLE service_columns add FOREIGN KEY(service_id) REFERENCES services(id) ON DELETE CASCADE;


DROP TABLE IF EXISTS column_metas;
CREATE TABLE column_metas (
  id serial PRIMARY KEY,
  column_id integer NOT NULL,
  name varchar(64) NOT NULL,
  seq integer NOT NULL,
  data_type varchar(8) NOT NULL DEFAULT 'string'
);

CREATE UNIQUE INDEX ux_column_id_name ON column_metas(column_id, name);
CREATE INDEX idx_column_id_seq ON column_metas(column_id, seq);
ALTER TABLE column_metas ADD FOREIGN KEY(column_id) REFERENCES service_columns(id) ON DELETE CASCADE;


DROP TABLE IF EXISTS labels;
CREATE TABLE labels (
  id serial PRIMARY KEY,
  label varchar(64) NOT NULL,
  src_service_id integer NOT NULL,
  src_column_name varchar(64) NOT NULL,
  src_column_type varchar(8) NOT NULL,
  tgt_service_id integer NOT NULL,
  tgt_column_name varchar(64) NOT NULL,
  tgt_column_type varchar(8) NOT NULL,
  is_directed boolean NOT NULL DEFAULT true,
  service_name varchar(64),
  service_id integer NOT NULL,
  consistency_level varchar(8) NOT NULL DEFAULT 'weak',
  hbase_table_name varchar(255) NOT NULL DEFAULT 's2graph',
  hbase_table_ttl integer,
  schema_version varchar(8) NOT NULL default 'v3',
  is_async boolean NOT NULL default false,
  compressionAlgorithm varchar(64) NOT NULL DEFAULT 'gz'
);

CREATE UNIQUE INDEX ux_label ON labels(label);
CREATE INDEX idx_src_column_name ON labels(src_column_name);
CREATE INDEX idx_tgt_column_name ON labels(tgt_column_name);
CREATE INDEX idx_src_service_id ON labels(src_service_id);
CREATE INDEX idx_tgt_service_id ON labels(tgt_service_id);
CREATE INDEX idx_service_name ON labels(service_name);
CREATE INDEX idx_service_id ON labels(service_id);
ALTER TABLE labels add FOREIGN KEY(service_id) REFERENCES services(id);



DROP TABLE IF EXISTS label_metas;
CREATE TABLE label_metas (
  id serial PRIMARY KEY,
  label_id integer NOT NULL,
  name varchar(64) NOT NULL,
  seq integer NOT NULL,
  default_value varchar(64) NOT NULL,
  data_type varchar(8) NOT NULL DEFAULT 'long'
);

CREATE UNIQUE INDEX ux_label_id_name ON label_metas(label_id, name);
CREATE INDEX idx_label_id_seq ON label_metas(label_id, seq);
ALTER TABLE label_metas ADD FOREIGN KEY(label_id) REFERENCES labels(id) ON DELETE CASCADE;




DROP TABLE IF EXISTS label_indices;
CREATE TABLE label_indices (
  id serial PRIMARY KEY,
  label_id integer NOT NULL,
  name varchar(64) NOT NULL DEFAULT '_PK',
  seq integer NOT NULL,
  meta_seqs varchar(64) NOT NULL,
  formulars varchar(255) DEFAULT NULL
);

CREATE UNIQUE INDEX ux_label_id_seq ON label_indices(label_id,meta_seqs);
CREATE UNIQUE INDEX ux_label_id_name_2 ON label_indices(label_id,name);
ALTER TABLE label_indices ADD FOREIGN KEY(label_id) REFERENCES labels(id) ON DELETE CASCADE;


DROP TABLE IF EXISTS experiments;
CREATE TABLE experiments (
  id serial PRIMARY KEY,
  service_id integer NOT NULL,
  service_name varchar(128) NOT NULL,
  name varchar(64) NOT NULL,
  description varchar(255) NOT NULL,
  experiment_type varchar(8) NOT NULL DEFAULT 'u',
  total_modular integer NOT NULL DEFAULT 100
);

CREATE UNIQUE INDEX ux_service_id_name_3 ON experiments(service_id, name);
ALTER TABLE experiments ADD FOREIGN KEY(service_id) REFERENCES services(id) ON DELETE CASCADE;



DROP TABLE IF EXISTS buckets;
CREATE TABLE buckets (
  id serial PRIMARY KEY,
  experiment_id integer NOT NULL,
  uuid_mods varchar(64) NOT NULL,
  traffic_ratios varchar(64) NOT NULL,
  http_verb varchar(8) NOT NULL,
  api_path text NOT NULL,
  uuid_key varchar(128),
  uuid_placeholder varchar(64),
  request_body text NOT NULL,
  timeout integer NOT NULL DEFAULT 1000,
  impression_id varchar(64) NOT NULL,
  is_graph_query boolean NOT NULL DEFAULT true
);
CREATE UNIQUE INDEX ux_impression_id on buckets(impression_id);
CREATE INDEX idx_experiment_id ON buckets(experiment_id);
CREATE INDEX idx_impression_id ON buckets(impression_id);


DROP TABLE IF EXISTS counter;
CREATE TABLE counter (
  id serial PRIMARY KEY,
  use_flag boolean NOT NULL DEFAULT false,
  version smallint NOT NULL DEFAULT 1,
  service varchar(64) NOT NULL DEFAULT '',
  action varchar(64) NOT NULL DEFAULT '',
  item_type integer NOT NULL DEFAULT 0,
  auto_comb boolean NOT NULL DEFAULT true,
  dimension varchar(1024) NOT NULL,
  use_profile boolean NOT NULL DEFAULT false,
  bucket_imp_id varchar(64) DEFAULT NULL,
  use_exact boolean NOT NULL DEFAULT true,
  use_rank boolean NOT NULL DEFAULT true,
  ttl integer NOT NULL DEFAULT 172800,
  daily_ttl integer DEFAULT NULL,
  hbase_table varchar(1024) DEFAULT NULL,
  interval_unit varchar(1024) DEFAULT NULL,
  rate_action_id integer DEFAULT NULL,
  rate_base_id integer DEFAULT NULL,
  rate_threshold integer DEFAULT NULL
);

CREATE UNIQUE INDEX svc ON counter(service, action);
CREATE INDEX rate_action_id ON counter(rate_action_id);
CREATE INDEX rate_base_id ON counter(rate_base_id);

ALTER TABLE counter ADD FOREIGN KEY(rate_action_id) REFERENCES counter(id) ON DELETE NO ACTION ON UPDATE NO ACTION;
ALTER TABLE counter ADD FOREIGN KEY(rate_base_id) REFERENCES counter(id) ON DELETE NO ACTION ON UPDATE NO ACTION;


GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO graph;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO graph;
