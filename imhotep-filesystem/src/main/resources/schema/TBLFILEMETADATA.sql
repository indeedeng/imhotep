CREATE TABLE IF NOT EXISTS tblFileMetadata (
  shard_name      VARCHAR(255) NOT NULL,
  file_path       VARCHAR(255) NOT NULL,
  size            BIGINT       NOT NULL,
  timestamp       BIGINT       NOT NULL,
  checksum        VARCHAR(128),
  archive_offset  BIGINT       NOT NULL,
  compressor_type VARCHAR(16)  NOT NULL,
  archive_name    VARCHAR(255),
  is_file         BOOLEAN      NOT NULL,
  packed_size     BIGINT       NOT NULL,
  PRIMARY KEY (shard_name, file_path)
);