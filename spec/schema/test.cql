CREATE TABLE things (
  owner INT,
  id INT,
  val VARCHAR,
  PRIMARY KEY (owner, id)
) WITH CLUSTERING ORDER BY (id DESC)
