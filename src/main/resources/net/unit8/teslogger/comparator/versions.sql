CREATE TABLE IF NOT EXISTS versions (
   id identity not null,
   table_name varchar(255) not null,
   created_at timestamp default CURRENT_TIMESTAMP() not null,
   PRIMARY KEY(id)
)