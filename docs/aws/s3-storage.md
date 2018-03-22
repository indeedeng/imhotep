---
layout: default
title: S3 Storage Setup
permalink: /docs/aws/s3-storage/
---

Create the following new S3 buckets. These buckets should be created in the same region in which you will run the EC2 instances for Imhotep.

1. a build bucket to store your uploaded data

2. a data bucket to store your Imhotep datasets

3. a cache bucket to store query cache data (configure this bucket to expire objects after 1 day, by adding a Lifecycle rule on the Management tab)


Create a user (in **IAM > Users**) with programmatic access and full access to your new S3 buckets. Save the access key ID and secret for later use.

**Next Step**: [Set up SSH access and security groups](../security/)
