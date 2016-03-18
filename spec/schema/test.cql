CREATE TABLE things (
  owner INT,
  id INT,
  val VARCHAR,
  PRIMARY KEY (owner, id)
) WITH CLUSTERING ORDER BY (id DESC);

CREATE TABLE type_testers (
  id VARCHAR,
  int_value INT,
  varint_value VARINT,
  bigint_value BIGINT,
  float_value FLOAT,
  double_value DOUBLE,
  decimal_value DECIMAL,
  ascii_value ASCII,
  varchar_value VARCHAR,
  text_value TEXT,
  blob_value BLOB,
  boolean_value BOOLEAN,
  timestamp_value TIMESTAMP,
  uuid_value UUID,
  timeuuid_value TIMEUUID,
  inet_value INET,
  list_value LIST<VARCHAR>,
  set_value SET<VARCHAR>,
  map_value MAP<VARCHAR, VARCHAR>,
  PRIMARY KEY (id)
);

CREATE TABLE type_tester_counters (
  id VARCHAR,
  counter_value COUNTER,
  PRIMARY KEY (id)
);
