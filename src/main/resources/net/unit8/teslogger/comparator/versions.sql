CREATE TABLE IF NOT EXISTS versions (
   id identity not null,
   created_at timestamp default CURRENT_TIMESTAMP() not null,
   PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS version_tables (
   version_id bigint not null,
   table_name varchar(255) not null,
   PRIMARY KEY(version_id, table_name)
)