---
layout: default
title: Quick Start
permalink: /docs/quick-start/
---

## Get Imhotep

### AWS configuration
Prerequisites: AWS account, S3 buckets for data storage

Create stack on AWS: In cloud formation (in aws), point to the imhotep-aws-config script to display the setup form.

Fields - get from the config script. 

When setup on AWS is complete, you should see the URLs for IUploader and IQL.

### Data upload

Use the Imhotep TSV Uploader to make your data available in Imhotep.

Prerequisites: TSV files with integer and/or string fields. 

If a field value includes a decimal, the value becomes a string and you will not be able to use it as a metric. Workaround: use millicent as a value.

VERIFY: Uploader detects integer or string.

Top row of file is required to include the field names. Plug for importance of naming conventions/consistency.

QUESTION: where are the logs for the web app and the converter? The location is configurable.

Upload an index:

1. At the bottom of the column of indices, enter a name for your new index and click +.
2. Browse to the index file and click **Upload TSV**.

Status: Waiting, Indexed, Failed QUESTION: are there others?

Delete an index:

Select the index name and click the trash icon.

## IQL queries

When your indices are uploaded, browse to IQL to perform queries.


