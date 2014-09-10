---
layout: default
title: Quick Start
permalink: /docs/quick-start/
---

This section guides you through the process of configuring an AWS stack and uploading your data to a web app that allows you to access your data in Imhotep using Imhotep Query Language (IQL). 

## AWS Configuration

###Prerequisites
You must have an AWS account and four S3 buckets for data storage. For this configuration, you will use AWS CloudFormation, Amazon S3 and Amazon EC2.

### Setup
Use AWS CloudFormation to create a stack on AWS: point to the imhotep-aws-config script to define the parameters of your Imhotep configuration. 

| Parameter | Description |
| ------------- | --------- |
| InstanceType | Defines the memory, CPU, storage capacity, and hourly cost for the Imhotep instance. Valid values: m3.xlarge, m3.2xlarge, c3.2xlarge, c3.4xlarge, c3.8xlarge, r3.large, r3.xlarge, r3.2xlarge, r3.4xlarge, r3.8xlarge |
| IPrange | IP address range for access to Imhotep. The range must be a valid IP CIDR range of the form x.x.x.x/x. |
| KeyName | Name of an existing EC2 key pair to enable SSH access to the Imhotep instances. |
| NumImhotepInstances | By default, the setup script creates two Imhotep instances to handle failover. |
| s3BuildBucket | Contains your data from TSV Uploader. |
| s3cacheBucket | Contains your cached results from IQL queries. |
| s3dataBucket | Contains your Imhotep indexes. |
| s3imhotepBucket | Contains Imhotep jars. |
| s3Key | Key for bucket access. |
| s3Secret | Key used with the s3Key for bucket access. |
| SSHLocation | IP address range for SSH access to Imhotep EC2 instances. The range must be a valid IP CIDR range of the form x.x.x.x/x. |

When the setup is successful, URLs are available for the TSV Uploader and IQL tools.  [TSV Uploader]({{ site.baseurl }}/docs/data-upload) allows you to upload your data to Imhotep. IQL allows you to run Imhotep queries.
