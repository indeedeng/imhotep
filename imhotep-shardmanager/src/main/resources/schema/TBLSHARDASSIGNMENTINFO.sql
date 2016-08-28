CREATE TABLE IF NOT EXISTS tblShardAssignmentInfo (
  dataset       VARCHAR(128) NOT NULL,
  id            VARCHAR(128) NOT NULL,
  assigned_node VARCHAR(128) NOT NULL,
  version       BIGINT       NOT NULL,
  PRIMARY KEY (dataset, id, assigned_node),
);

CREATE INDEX IF NOT EXISTS tblShardAssignmentInfo_assigned_node_idx
  ON tblShardAssignmentInfo (assigned_node);
