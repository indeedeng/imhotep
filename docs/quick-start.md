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

When the setup is successful, URLs are available for the TSV Uploader and IQL tools.  TSV Uploader allows you to upload your data to Imhotep. IQL allows you to run Imhotep queries.

## TSV Uploader

Use TSV Uploader to make your data available in Imhotep.

### Creating an Index
1. Open a browser and navigate to the TSV Uploader URL provided from the AWS configuration. A list of indices that are available in the system appears on the left side of the page. 
2. Scroll to the bottom of this list and enter a name for your new index in the text entry box. Use lowercase characters matching regex [a-z0-9]+ for your index name.
3. Click + to create the index.

The name of your new index appears in the list. When you first add the index, it is empty until you upload a data file. 

### Uploading a Data File
1. In TSV Uploader, click the index name.
2. In the search field near the top of the page, click **Upload TSV** and browse to the TSV file that contains your index data. 
3. Repeat this step to upload additional data files to your index. 

TSV Uploader indexes the file for use in Imhotep. When the process completes successfully, indexed shows as the status of the file. If the process fails, failed shows as the status. Errors are written to a .error.log file, which you can download to your computer and view. 

[Learn about data file requirements for TSV Uploader]({{ site.baseurl }}/docs/data-file-requirements).
