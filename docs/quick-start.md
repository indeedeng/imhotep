---
layout: default
title: Quick Start
permalink: /docs/quick-start/
---

This section guides you through the process of configuring an AWS stack and uploading your data to a web app that allows you to access your data in Imhotep using Imhotep Query Language (IQL).

##AWS Configuration

###Prerequisites
You must have an AWS account and three S3 buckets for data storage. For this configuration, you will use AWS CloudFormation, Amazon S3 and Amazon EC2.

NOTE: If you build your own version of Imhotep, you will use the s3imhotepBucket for the Imhotep executables.

###Setup
Use AWS CloudFormation to create a stack on AWS.

Point to the `imhotep-aws-config` script at the following URL to define the parameters of your Imhotep configuration:

`http://imhotep-cloud-setup.s3.amazonaws.com/cloudFormation_cluster_launch.json`

The script prompts you to define the following parameters:

| Parameter | Description |
| ------------- | --------- |
| `InstanceType` | Defines the memory, CPU, storage capacity, and hourly cost for the Imhotep instance. Valid values: `m3.xlarge`, `m3.2xlarge`, `c3.2xlarge`, `c3.4xlarge`, `c3.8xlarge`, `r3.large`, `r3.xlarge`, `r3.2xlarge`, `r3.4xlarge`, `r3.8xlarge` |
| `IPrange` | IP address range for access to Imhotep. The range must be a valid IP CIDR range of the form `x.x.x.x/x`. |
| `KeyName` | Name of an existing EC2 key pair to enable SSH access to the Imhotep instances. |
| `NumImhotepInstances` | By default, the setup script creates two Imhotep instances to handle failover. |
| `s3BuildBucket` | Contains your data from TSV Uploader. |
| `s3cacheBucket` | Contains your cached results from IQL queries. |
| `s3dataBucket` | Contains your Imhotep indexes. |
| `s3imhotepBucket` | Contains Imhotep jars. |
| `s3Key` | Key for bucket access. |
| `s3Secret` | Key used with the s3Key for bucket access. |
| `SSHLocation` | IP address range for SSH access to Imhotep EC2 instances. The range must be a valid IP CIDR range of the form `x.x.x.x/x`. |

When the setup is successful, URLs are available for the Imhotep TSV Uploader and IQL tools.  Imhotep TSV Uploader allows you to upload your data to Imhotep. [IQL allows you to run Imhotep queries.]({{ site.baseurl }}/docs/overview)

## Imhotep TSV Uploader

Use TSV Uploader to make your data available in Imhotep. TSV Uploader converts the TSV files into indexes that Imhotep can use and moves the indexes to the correct location so that Imhotep can access them. 

### Creating an Index
1. Open a browser and navigate to the Imhotep TSV Uploader URL provided from the AWS configuration. A list of indexes that are available in the system appears on the left side of the page. 
2. Scroll to the bottom of this list and enter a name for your new index in the text entry box. Use lowercase characters matching regex [a-z0-9]+ for your index name.
3. Click **+** to create the index.

The name of your new index appears in the list. When you first add the index, it is empty until you upload a data file. An index is not created on Imhotep until you upload a data file and a shard is created.

### Uploading a Data File
1. In Imhotep TSV Uploader, click the index name.
2. In the search field near the top of the page, click **Upload TSV** and browse to the TSV file that contains your index data. Repeat this step to upload additional data files to your index. To upload multiple files at one time, with the index name selected, drag and drop the files to the TSV Uploader window.

NOTE: If you upload a TSV file to the wrong index, you must manually remove the shard with the index from Imhotep. [Learn how]({{ site.baseurl }}/docs/troubleshooting). 

When the process completes successfully, **indexed** shows as the status of the file. If the process fails, **failed** shows as the status. Errors are written to a `.error.log` file, which you can download to your computer and view. 

[Learn about data file requirements for TSV Uploader]({{ site.baseurl }}/docs/data-file-requirements).

### Deleting Files from TSV Uploader
To delete a data file, select *indexName* > *dataFileName* and click the trash can. 

To delete an index, select *indexName* and click the trash can.

NOTE: Deleting a data file or index from TSV Uploader does not delete the index from Imhotep. TSV Uploader shows the list of data files for two weeks after a file's upload date.

##Imhotep Query Language
Use IQL to run queries on the data you uploaded to your indexes. To launch IQL, open a browser and navigate to the TSV Uploader URL provided from the AWS configuration. Constructing an IQL query follows these general steps:

1. Formulate your question.
2. Select your index and the date range.
3. Enter the query.
4. Select how you want to group your data. Groups show as rows in tabular data.
5. Choose one or multiple metrics for your data. Metrics show as a columns in tabular data.
6. Click **Run**.

[Learn more about using IQL]({{ site.baseurl }}/docs/overview).
