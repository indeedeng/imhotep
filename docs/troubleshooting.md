---
layout: default
title: Troubleshooting
permalink: /docs/troubleshooting/
---

This section highlights errors you might encounter when you construct a query. 

##File Upload Errors

###I don't see my dataset in the client
Try clearing the cache if you don't see your new dataset in the IQL web client.

###Files fail to upload
Upload failures in TSV Uploader are marked as failed.

Ensure that the bucket names you defined when you created the stack match the S3 bucket names you created prior to setup. For example, if the error log shows that TSV Uploader couldn’t upload the converted files to the S3 bucket, you might have entered the incorrect name for the **BuildBucket** parameter during stack creation. In this case, you must recreate the stack. Don't worry, you won't lose any data, but do ensure that you point to the same S3 buckets when you recreate the stack.

###A TSV file was uploaded to the wrong dataset

If a TSV file is indexed in the wrong dataset, you must delete the shard that contains the new data:

1. Determine the time range of the shard by running an IQL query for fields in the TSV file.
2. Sign into AWS and navigate to your S3 data bucket. 
3. Open the folder for the dataset that contains the new data from the TSV file you uploaded.
4. From the list of compressed shards, locate the shard with the timestamp from your IQL query.
5. Delete the shard folder and corresponding document.

##Query Errors

 `=, :, !=, =~, !=~, (, *, \, %, +, -, /, >=, >, <=, <, in, not or not in expected, EOF encountered.`

You entered invalid text after a field name. Review the query syntax to ensure the query is not missing the operator (: or =). If the syntax is correct, add quotations around the field value. Example: country:”united states” <br><br>

`INTEGER, string literal or IDENTIFIER expected, EOF encountered.`

One of the fields has a typo.

##Slow Queries

If your queries are extremely slow, you must have a lot of data. Here are some tips for handling queries on large datasets.

####Add Imhotep machines to the cluster
Recreate the stack to increase the value of `NumImhotepInstances`. If you have already uploaded data into your S3 buckets, ensure that you point to the same S3 buckets when you recreate the stack. You don't need to upload your data again.

####Test on a small time range
Start small and then ramp up to the required range if performance is sufficient. 

| Use |  Do not use |
| ------ | --------|
| `1h today` |  `180d today` |

####Determine the actual number of expected groups
If you think your query will return a large number of groups, run a DISTINCT query to return the actual number of expected groups before grouping your data:

`1h today select distinct(accountid)`

If the number of expected groups is a value that your system can handle, run the **group by** query:

`1h today group by accountid`

####Make the largest group the last
If ascending order on all columns from left to right is not necessary, try making the largest group the last grouping and make it non-exploded by adding square brackets to the field name. This allows the result to be streamed instead of stored in memory.
<table>
  <tr>
    <th>Use</th>
    <th>Do not use</th>
  </tr>
  <tr>
    <td valign="top">`group by country, city[]`</td>
    <td valign="top"> `group by country, city`<br>
`group by city, country[]` <br>
`group by city[500000], country[50]` </td>
  </tr>
</table>



The `group by city[500000], country[50]` is especially problematic because IQL can’t verify in advance how many terms will be returned. If the requested number is too high, IQL uses too much memory and requires time to recover.

####Avoid using DISTINCT for large queries
Don’t use distinct() as a metric with a large amount of data if you are using the **group by** filter with a large amount of data. 

#### Heap memory size
The number of rows IQL can return on non-streaming queries depends on the heap size allocated to IQL.

