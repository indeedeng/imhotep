## Table of Contents

* [Introduction](#introduction)
* [What You’ll Need](#what-youll-need)
* [Install Docker](#install-docker)
* [Install Docker Compose](#install-docker-compose)
* [Get the Imhotep Docker Images](#get-the-imhotep-docker-images)
* [Run Docker Compose](#run-docker-compose)
* [Use the Tools](#use-the-tools)
* [Appendix A: Architecture](#appendix-a-architecture)
* [Appendix B: Container Troubleshooting](#appendix-b-container-troubleshooting)

<sub>Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc.go)</sub>

## Introduction

Imhotep is a large-scale analytics platform built by Indeed. To learn more, look at the [Imhotep documentation](http://opensource.indeedeng.io/imhotep/).

If you want to quickly evaluate Imhotep, you can install all the components on a single machine using docker. The [Architecture section](#appendix-a-architecture) below describes the components in more detail.

## What You’ll Need

* the ability to install Docker if you don’t already have it
* about 10 GB of free disk space

## Install Docker

### Linux

If you are running a linux distribution, you can probably install using the [get.docker.com script](https://get.docker.com/).
```
curl -sSL https://get.docker.com/ | sh
```
### Mac OS

Download and install [Docker for Mac](https://www.docker.com/products/docker#/mac).

### Windows

Download and install [Docker for Windows](https://www.docker.com/products/docker#/windows).

## Install Docker Compose

Follow the [Install Docker Compose](https://docs.docker.com/compose/install/) instructions to install Docker Compose for your platform.

## Get the Imhotep Docker Images

### Option 1: Pull Images from Docker Hub

This option allows you to download pre-built images, which may save you some time building.

Download the docker-compose.yml file into a new directory:
```
mkdir imhotep-docker
cd imhotep-docker
wget https://raw.githubusercontent.com/indeedeng/imhotep-docker/master/docker-compose.yml
docker-compose pull
```

### Option 2: Build Images from Github

Clone or download the [imhotep-docker](https://github.com/indeedeng/imhotep-docker) project.

Option 1. Clone with SSH:
```
git clone git@github.com:indeedeng/imhotep-docker.git
```
Option 2. Clone with HTTPS:
```
git clone https://github.com/indeedeng/imhotep-docker.git
```
Option 3. Download and expand zip archive:
```
wget https://github.com/indeedeng/imhotep-docker/archive/master.zip
unzip master.zip
```

Before building the docker images, you may want to consider using the --squash option to
save disk space. If your version of Docker has --squash support ([experimental in 1.13](https://blog.docker.com/2017/01/whats-new-in-docker-1-13/#h.al9vz4gnifqz)),
you can set this environment variable to enable the option:
```
export DOCKER_BUILD_OPTS=--squash
```

Until we confirm that Imhotep works with Java 8, you'll also need to download a JDK 7 RPM
(jdk-7u80-linux-x64.rpm) from Oracle and copy it into the base-java7/ directory, e.g.
```
cp ~/Downloads/jdk-7u80-linux-x64.rpm imhotep-docker/base-java7/
```

Run the provided bash script to build and install the Imhotep docker images locally.
```
cd imhotep-docker
./build-images.sh
```
This script will run for a while, and when it is complete, you will have four imhotep images available. 

* local/imhotep-frontend:centos6
* local/imhotep-daemon:centos6
* local/imhotep-cdh5-hdfs:centos6
* local/imhotep-zookeeper:centos6

## Run Docker Compose

If you would like to run the Imhotep web tools on a port other than 80, you can either set an environment variable or edit the .env file. For example, to run on 8080 from a bash shell:
```
export IQL_PORT=8080
```
Now you are ready to run the four docker containers that make up the full Imhotep stack.
```
docker-compose up
```
You will see a lot of log messages while the stack starts up.

The first time you run docker-compose, it will create a docker volume for the HDFS storage. That way, if you restart your containers, your data will still be available. Usually the last messages you see on first run look like this:
```
hadoop_1     | Started Hadoop secondarynamenode:[  OK  ]
hadoop_1     | Setting up typical users
hadoop_1     | Creating /imhotep/ in HDFS
hadoop_1     | Refresh user to groups mapping successful
hadoop_1     | /
hadoop_1     | /imhotep
hadoop_1     | /imhotep/imhotep-build
hadoop_1     | /imhotep/imhotep-build/iupload
hadoop_1     | /imhotep/imhotep-build/iupload/failed
hadoop_1     | /imhotep/imhotep-build/iupload/indexedtsv
hadoop_1     | /imhotep/imhotep-build/iupload/tsvtoindex
hadoop_1     | /imhotep/imhotep-data
hadoop_1     | /imhotep/iql
hadoop_1     | /imhotep/iql/shortlinks
hadoop_1     | /user
hadoop_1     | /user/root
hadoop_1     | /user/shardbuilder
hadoop_1     | /user/tomcat7
```
These messages indicate HDFS is ready to be used by the Imhotep components.

Due to a quirk of relative startup times, after first-time startup you'll need to restart the Tomcat in the frontend container in order for the short-link feature to work:
```
frontend_id=`docker ps | grep imhotep-frontend | cut -f1 -d\ `
docker exec -i $frontend_id service tomcat stop
docker exec -i $frontend_id service tomcat start
```

You should now be able to access the web tools for Imhotep:

* IUpload: [http://localhost/iupload/](http://localhost/iupload/)
* IQL: [http://localhost/iql/](http://localhost/iql/) 

(Be sure to specify the correct port if you changed the default.)

## Use the Tools

Now you are ready to upload TSV time-series data (using IUpload, [start here](http://opensource.indeedeng.io/imhotep/docs/quick-start/#imhotep-tsv-uploader)) and run queries on your data sets (using IQL, [start here](http://opensource.indeedeng.io/imhotep/docs/quick-start/#iql-web-client)).

## Appendix A: Architecture

### ImhotepDaemon (a.k.a. Imhotep Server)

The ImhotepDaemon is the back-end component responsible for looking servicing query requests. Adding instances of ImhotepDaemon is the primary way to maintain high performance with large amounts of data and increased load.

This component is implemented in Java and depends on the zookeeper cluster (to coordinate with other components) and the storage layer (HDFS or S3, to pull down data shards for serving).

### Imhotep Frontend Components

#### IQL Webapp

The IQL webapp presents a web-based user interface for issuing IQL queries. Usage of this tool is described in the [Quick Start guide](http://opensource.indeedeng.io/imhotep/docs/quick-start/#iql-web-client).

This component is implemented in Java and typically runs in the Tomcat7 servlet container behind the Apache web server. It depends on the zookeeper cluster (to find ImhotepDaemon instances) and ImhotepDaemon instances (to service queries).

#### IUpload Webapp (a.k.a TSV uploader)

The IUpload webapp presents a web-based user interface for uploading data in TSV or CSV format into the Imhotep system. Usage of this tool is described in the [Quick Start guide](http://opensource.indeedeng.io/imhotep/docs/quick-start/#imhotep-tsv-uploader).

This component is implemented in Java and typically runs in the Tomcat7 servlet container behidn the Apache web server. It depends on the storage layer (HDFS or S3) to place uploaded files. It is optional; TSV/CSV data can be placed directly in the storage layer following conventions described in the [Quick start guide](http://opensource.indeedeng.io/imhotep/docs/quick-start/#imhotep-tsv-uploader).

#### Shard Builder (a.k.a. TSV converter)

The shard builder typically runs as a scheduled cron job and handles converting TSV or CSV files that have been uploaded to the storage layer into data shards for consumption by the ImhotepDaemon instances.

This component is implemented in Java and depends on the storage layer (HDFS or S3, to retrieve uploaded data and store converted data).

### Storage Layer

The storage layer for Imhotep can be HDFS (Apache Hadoop File System) or S3 (Amazon Simple Storage Service). S3 is probably preferable if you are running in AWS. If not running in AWS, you should probably choose HDFS, as we do for this docker evaluation version of the stack.

Imhotep has been tested with the [CDH5 distribution](https://www.cloudera.com/downloads/cdh/5-10-0.html) of Hadoop.

### Zookeeper Cluster

The zookeeper cluster is used for coordination among the ImhotepDaemon instances and the IQL webapp frontend.

Imhotep has been tested with Zookeeper 3.4.5 from the CDH 5 distribution ([download link](http://archive.cloudera.com/cdh5/cdh/5/zookeeper-3.4.5-cdh5.10.0.tar.gz)).

## Appendix B: Container Troubleshooting

### Connecting to containers

You can run `docker ps` to see your running docker containers. You can access the containers by interactively running bash in them (`docker exec -it <ID> bash`). Here’s an example of connecting to the imhotep-frontend container:

```
$ docker ps
c91cbbc7722a        local/imhotep-cdh5-hdfs:centos6   "/bin/sh -c hdfs-s..."   23 seconds ago      Up 3 seconds        8020/tcp             imhotepimages_hadoop_1
d9e4c6eabc3f        local/imhotep-zookeeper:centos6   "/opt/zookeeper/bi..."   23 seconds ago      Up 3 seconds        2181/tcp             imhotepimages_zookeeper_1
58a278dcc7a2        local/imhotep-daemon:centos6      "/bin/sh -c /opt/i..."   23 seconds ago      Up 4 seconds        12345/tcp            imhotepimages_daemon_1
4fdb16fef6ae        local/imhotep-frontend:centos6    "/bin/sh -c ./star..."   23 seconds ago      Up 8 seconds        0.0.0.0:80->80/tcp   imhotepimages_frontend_1

$ docker exec -it 4fdb16fef6ae bash
[root@4fdb16fef6ae imhotepTsvConverter]# ls /opt/imhotepTsvConverter/logs/
shardBuilder-error.log  shardBuilder.log
[root@4fdb16fef6ae imhotepTsvConverter]# ls /opt/tomcat7/logs/
catalina.2017-02-27.log      iql-error.log             localhost_access_log.2017-02-27.txt
catalina.out                 iql.log                   manager.2017-02-27.log
host-manager.2017-02-27.log  localhost.2017-02-27.log
```
### imhotep-frontend

This container runs IUpload, IQL, and the Shard Builder (TSV converter).

* IQL and IUpload webapp WAR deployment files: /opt/tomcat7/webapps/
* Web application log files: /opt/tomcat7/logs/
* Shard builder cron script: /opt/imhotepTsvConverter/tsvConverter.sh
* Shard builder logs: /opt/imhotepTsvConverter/logs/
* HDFS configuration for Tomcat: /opt/tomcat_shared/core-site.xml
* HDFS configuration for shard builder: /opt/imhotepTsvConverter/conf/core-site.xml

### imhotep-daemon

This container runs the Imhotep server process. 

* Script that runs the process: /opt/imhotep/imhotep.sh
* Daemon log file: /var/data/imhotep/logs/ImhotepDaemon_log4j.log
* HDFS configuration: /opt/imhotep/core-site.xml
* Various data files: /var/data/

### imhotep-cdh5-hdfs

This container runs HDFS in a single server mode. You can connect to this container and run `hdfs` commands to interact with the files there. Example:
```
$ docker exec -it c91cbbc7722a bash
[root@c91cbbc7722a /]# hdfs dfs -find /
/
/imhotep
/imhotep/imhotep-build
/imhotep/imhotep-build/iupload
/imhotep/imhotep-build/iupload/failed
/imhotep/imhotep-build/iupload/indexedtsv
/imhotep/imhotep-build/iupload/tsvtoindex
/imhotep/imhotep-data
/imhotep/iql
/imhotep/iql/shortlinks
/user
/user/root
/user/shardbuilder
/user/tomcat7
```
### imhotep-zookeeper

This container runs a single zookeeper node. You probably won’t need to connect to it.
