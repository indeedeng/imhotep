---
layout: default
title: Troubleshooting
permalink: /docs/troubleshooting/
---

This page highlights file upload and query errors and their workarounds.

## Page Contents

* [AWS Stack Management](#aws-stack-management)
* [File Upload Errors](#file-upload-errors)
* [Query Errors](#query-errors)
* [Slow Queries](#slow-queries)
* [Performance Considerations for IQL Usage](#performance-considerations-for-iql-usage)

<sub>Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc.go)</sub>

## AWS Stack Management

### My AWS user account is not authorized to create IAM resources
The stack creation procedure requires you to create an Identity and Access Management (IAM) resource. If you are not signed into AWS as root (or a user with permission to create IAM resources), setup fails with `CREATE_FAILED` and `ROLLBACK_COMPLETE`.

### I deleted my stack and still see an S3 bucket
If you delete your stack, an S3 bucket might still exist. You can delete the bucket in **S3** in the AWS Management Console. Note that if files are still in the bucket, they will be deleted after a day.

## File Upload Errors

### I don't see my dataset in the client
Try clearing the cache if you don't see your new dataset in the IQL web client: select **Settings > Clear cache**.

### Files fail to upload
Upload failures in TSV Uploader are marked as failed.

Ensure that the bucket names you defined when you created the stack match the S3 bucket names you created prior to setup. For example, if the error log shows that TSV Uploader couldn’t upload the converted files to the S3 bucket, you might have entered the incorrect name for the **BuildBucket** parameter during stack creation. In this case, you must recreate the stack. Don't worry, you won't lose any data, but do ensure that you point to the same S3 buckets when you recreate the stack.

### A TSV file was uploaded to the wrong dataset

If a TSV file is indexed in the wrong dataset, you must delete the shard that contains the new data:

1. Determine the time range of the shard by running an IQL query for fields in the TSV file.
2. Sign into AWS and navigate to your S3 data bucket. 
3. Open the folder for the dataset that contains the new data from the TSV file you uploaded.
4. From the list of compressed shards, locate the shard with the timestamp from your IQL query.
5. Delete the shard folder and corresponding document.

## Query Errors

 <code>=, :, !=, =~, !=~, (, *, \, %, +, -, /, >=, >, <=, <, in, not or not in expected, EOF encountered.</code>

You entered invalid text after a field name. Review the query syntax to ensure the query is not missing the operator (: or =). If the syntax is correct, add quotations around the field value. Example: country:”united states” <br><br>

<code>INTEGER, string literal or IDENTIFIER expected, EOF encountered.</code>

One of the fields has a typo.

## Slow Queries

If your queries are extremely slow, you must have a lot of data. Here are some tips for handling queries on large datasets.

### Add Imhotep machines to the cluster
Recreate the stack to increase the value of <code>NumImhotepInstances</code>. If you have already uploaded data into your S3 buckets, ensure that you point to the same S3 buckets when you recreate the stack. You don't need to upload your data again.

### Test on a small time range
Start small and then ramp up to the required range if performance is sufficient. 

| Use |  Do not use |
| ------ | --------|
| `1h today` |  `180d today` |

### Determine the actual number of expected groups
If you think your query will return a large number of groups, run a DISTINCT query to return the actual number of expected groups before grouping your data:

`1h today select distinct(accountid)`

If the number of expected groups is a value that your system can handle, run the **group by** query:

`1h today group by accountid`

### Make the largest group the last
If ascending order on all columns from left to right is not necessary, try making the largest group the last grouping and make it non-exploded by adding square brackets to the field name. This allows the result to be streamed instead of stored in memory.

<div class="table-wrapper">

<table>
  <tr>
    <th>Use</th>
    <th>Do not use</th>
  </tr>
  <tr>
    <td valign="top"><code>group by country, city[]</code></td>
    <td valign="top"> <code>group by country, city</code><br>
<code>group by city, country[]</code> <br>
<code>group by city[500000], country[50]</code></td>
  </tr>
</table>
</div>


The `group by city[500000], country[50]` is especially problematic because IQL can’t verify in advance how many terms will be returned. If the requested number is too high, IQL uses too much memory and requires time to recover.

### Avoid using DISTINCT for large queries
Don’t use distinct() as a metric with a large amount of data if you are using the **group by** filter with a large amount of data. 

### Heap memory size
The number of rows IQL can return on non-streaming queries depends on the heap size allocated to IQL.

## Performance Considerations for IQL Usage

### Query smaller time ranges
Test your query on a small time range first. Ramp up to the required range if performance is sufficient. 
For example, `FROM example 1h today ...` is preferable to `FROM example 180d today ...`.

When saving/bookmarking a query consider the time range being used. Queries auto-run when revisited, requiring you to wait for completion before adjusting the time range.

### Limit your use of regex
Using a regular expression (regex) allows for more flexible filtering, but is more costly than filtering on the exact terms using `field IN (terms)`.

If possible, use alternative fields rather than regex. For example, tokenized field m instead of myTerm or a single URL segment instead of the full path.

Frequently using regex to filter on a field can indicate a need for indexing more specialized views of that data in the index builder.

Try to combine multiple regex filters on the same field into a single regex. For example, use `ref!=~"(apple|ifa_|orange123).*"` instead of `ref!=~"apple.*" ref!=~"ifa_.*" ref!=~"orange123.*"`

Regex filtering becomes particularly expensive in the following situations: 
* when used on fields with a large number of distinct terms, such as myTerm
* when terms are long, such as URLs
* when querying long time periods containing a large number of shards

### Don't try to un-invert an index / be smart about grouping
IQL indices are stored inverted, making it unfeasible to see the original documents as rows. Many users who are used to tabular data attempt to un-invert an index by grouping by every available field. This is inefficient and not recommended. If you need access to complete rows of data, consider alternative data sources such as MySQL and HBase.

If you think a grouping may result in a large number of groups, use the **distinct()** query to get the actual number of expected groups. For example, `FROM example 1h today SELECT distinct(stringField)` returns the number of groups you would get by running `FROM example 1h today GROUP BY stringField`

Try to put the largest grouping as the last grouping. This allows the result of the last largest grouping to be streamed instead of all stored in memory. For example, since there are dozens of somethings but millions of unique queries, prefer **`GROUP BY something, q`** to **`GROUP BY q, something`** or **`GROUP BY q[500000], something[50]`**.

### Avoid distinct() when possible
Avoid distinct() when possible for heavy queries. The optimization of streaming the data in the last group by grouping doesn't work if you have any distinct() elements in SELECT. For example, `FROM example 1h today select distinct(stringField)` is acceptable but `FROM example 4w today GROUP BY q SELECT distinct(stringField)` is not preferred.

Although the output of distinct(field) is a single number, all the values for that field must be streamed from Imhotep to IQL. This creates a great deal of data when run over larger time ranges for fields with many unique terms.

### Don't click Run mutiple times
When you create your query and click **Run**, the request for the data is sent to Imhotep/IQL. Clicking **Run** multiple times or refreshing the page sends additional requests to the servers. Doing this with queries that are too heavy to complete successfully, for example because they exceed a limit, causes especially negative behavior: the queries do not get cached, rerun many times, and fail each time while consuming resources.

Re-running queries can cause you additional lost time.