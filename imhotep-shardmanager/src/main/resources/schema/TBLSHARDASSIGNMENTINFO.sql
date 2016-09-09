CREATE TABLE IF NOT EXISTS tblShardAssignmentInfo (
  id            IDENTITY PRIMARY KEY,
  dataset       VARCHAR(128) NOT NULL,
  shard_id      VARCHAR(128) NOT NULL,
  assigned_node VARCHAR(128) NOT NULL,
  timestamp     TIMESTAMP    NOT NULL,
);

CREATE INDEX IF NOT EXISTS tblShardAssignmentInfo_assigned_node_idx
  ON tblShardAssignmentInfo (assigned_node);

CREATE INDEX IF NOT EXISTS tblShardAssignmentInfo_timestamp_idx
  ON tblShardAssignmentInfo (dataset, timestamp);
