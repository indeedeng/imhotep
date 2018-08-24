CREATE TABLE `tblfields` (
  `dataset` varchar(100) COLLATE latin1_bin NOT NULL,
  `fieldname` varchar(256) COLLATE latin1_bin NOT NULL,
  `type` enum('INT','STRING','CONFLICT') COLLATE latin1_bin NOT NULL,
  `lastshardstarttime` bigint(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin COMMENT='Metadata about fields in Imhotep datasets';

ALTER TABLE `tblfields`
  ADD PRIMARY KEY (`dataset`,`fieldname`);

CREATE TABLE `tblshards` (
  `path` varchar(200) COLLATE latin1_bin NOT NULL,
  `numDocs` int(11) NOT NULL,
  `addedtimestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin COMMENT='List of shards available to Imhotep';

ALTER TABLE `tblshards`
  ADD PRIMARY KEY (`path`),
  ADD UNIQUE KEY `addedtimestamp` (`addedtimestamp`,`path`,`numDocs`);