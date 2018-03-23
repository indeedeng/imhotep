---
layout: default
title: Imhotep Architecture
permalink: /docs/architecture-overview/
---

## Page Contents

* [Imhotep Backend Component](#imhotep-backend-component)
* [Imhotep Frontend Components](#imhotep-frontend-components)
* [Dependencies](#dependencies)

<sub>Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc.go)</sub>

Learn more about the Imhotep architecture and the components necessary to run Imhotep.


## Imhotep Backend Component
The ImhotepDaemon (a.k.a. Imhotep Server) is the back-end Java service responsible for looking servicing query requests. Add instances of ImhotepDaemon to help maintain high performance with large amounts of data and increased load.

 *It depends on*: 

* A Zookeeper cluster to coordinate with other components

* A storage layer (HDFS or S3) to pull down data shards for serving

## Imhotep Frontend Components

### **IQL Webapp**
The IQL webapp is a web-based user interface for issuing IQL queries. 

Learn [how to use this tool](http://opensource.indeedeng.io/imhotep/docs/quick-start/#iql-web-client).

This Java webapp typically runs in the Tomcat7 servlet container behind the Apache web server. 

*It depends on*: 

* A Zookeeper cluster to find ImhotepDaemon instances 

* ImhotepDaemon instances to service queries

### **IUpload Webapp**
The IUpload Webapp (a.k.a. TSV uploader) is a web-based user interface for uploading data in TSV or CSV format into the Imhotep system. 

Learn [how to use this tool](http://opensource.indeedeng.io/imhotep/docs/quick-start/#imhotep-tsv-uploader).

This Java webapp typically runs in the Tomcat7 servlet container behind the Apache web server. 

*It depends on*: 

* A storage layer (HDFS or S3) to place uploaded files 

Optimally, you can directly place TSV/CSV data in the storage layer. To upload files directly to your S3 build bucket, place the files in the <strong>iupload/tsvtoindex/datasetName/</strong> directory. As they are processed, they are moved to <strong>iupload/indexedtsv/datasetName/</strong>. You can also view the files in TSV Uploader. 

Learn more about [uploading data](http://opensource.indeedeng.io/imhotep/docs/quick-start/#imhotep-tsv-uploader).

### **Shard Builder (TSV converter)**
The shard builder typically runs as a scheduled cron job and handles converting TSV or CSV files uploaded to the storage layer into data shards for consumption by the ImhotepDaemon instances.

This builder is implemented in Java.

*It depends on*:
 
* A storage layer (HDFS or S3) to retrieve uploaded data and store converted data

## Dependencies

### **Java**
The Imhotep components have been tested with [Java 7 from Oracle](http://www.oracle.com/technetwork/java/javase/downloads/java-archive-downloads-javase7-521261.html). 

### **Storage Options**
The storage layer for Imhotep can be HDFS (Apache Hadoop File System) or S3 (Amazon Simple Storage Service). 

If you plan on running Imhotep in AWS, use S3. Otherwise, choose HDFS, as we do for this [docker evaluation version of the stack](https://github.com/indeedeng/imhotep-docker/blob/master/README.md).

Imhotep has been tested with the [CDH5 distribution](https://www.cloudera.com/downloads/cdh/5-10-0.html) of Hadoop. 

### **Zookeeper**
The Zookeeper cluster is used for coordination among the ImhotepDaemon instances and the IQL webapp frontend.

Imhotep has been tested with Zookeeper 3.4.5 from the CDH 5 distribution. [Download here](http://archive.cloudera.com/cdh5/cdh/5/zookeeper-3.4.5-cdh5.10.0.tar.gz).


