---
layout: default
title: Imhotep Architecture
permalink: /docs/architecture-overview/
---

## Table of Contents

* [Imhotep Backend Component](#imhotep-backend-component)
* [Imhotep Frontend Components](#imhotep-frontend-components)
* [Storage Layer](#storage-layer)
* [Zookeeper Cluster](#zookeeper-cluster)

<sub>Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc.go)</sub>

## Imhotep Backend Component

The ImhotepDaemon (a.k.a. Imhotep Server) is the back-end component responsible for looking servicing query requests. Adding instances of ImhotepDaemon is the primary way to maintain high performance with large amounts of data and increased load.

This component is implemented in Java and depends on the zookeeper cluster (to coordinate with other components) and the storage layer (HDFS or S3, to pull down data shards for serving).

## Imhotep Frontend Components

### IQL Webapp

The IQL webapp presents a web-based user interface for issuing IQL queries. Usage of this tool is described in the [Quick Start guide](http://opensource.indeedeng.io/imhotep/docs/quick-start/#iql-web-client).

This component is implemented in Java and typically runs in the Tomcat7 servlet container behind the Apache web server. It depends on the zookeeper cluster (to find ImhotepDaemon instances) and ImhotepDaemon instances (to service queries).

### IUpload Webapp 

The IUpload Webapp (a.k.a TSV uploader) presents a web-based user interface for uploading data in TSV or CSV format into the Imhotep system. Usage of this tool is described in the [Quick Start guide](http://opensource.indeedeng.io/imhotep/docs/quick-start/#imhotep-tsv-uploader).

This component is implemented in Java and typically runs in the Tomcat7 servlet container behind the Apache web server. It depends on the storage layer (HDFS or S3) to place uploaded files. It is optional; TSV/CSV data can be placed directly in the storage layer following conventions described in the [Quick start guide](http://opensource.indeedeng.io/imhotep/docs/quick-start/#imhotep-tsv-uploader).

### Shard Builder (a.k.a. TSV converter)

The shard builder typically runs as a scheduled cron job and handles converting TSV or CSV files that have been uploaded to the storage layer into data shards for consumption by the ImhotepDaemon instances.

This component is implemented in Java and depends on the storage layer (HDFS or S3, to retrieve uploaded data and store converted data).

## Storage Layer

The storage layer for Imhotep can be HDFS (Apache Hadoop File System) or S3 (Amazon Simple Storage Service). S3 is probably preferable if you are running in AWS. If not running in AWS, you should probably choose HDFS, as we do for this docker evaluation version of the stack.

Imhotep has been tested with the [CDH5 distribution](https://www.cloudera.com/downloads/cdh/5-10-0.html) of Hadoop.

## Zookeeper Cluster

The zookeeper cluster is used for coordination among the ImhotepDaemon instances and the IQL webapp frontend.

Imhotep has been tested with Zookeeper 3.4.5 from the CDH 5 distribution ([download link](http://archive.cloudera.com/cdh5/cdh/5/zookeeper-3.4.5-cdh5.10.0.tar.gz)).



