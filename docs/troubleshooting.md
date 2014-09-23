---
layout: default
title: Troubleshooting
permalink: /docs/troubleshooting/
---

This section highlights errors you might encounter when you construct a query. To avoid overloading the tool, read about [performance considerations and best practices][best-practices].

##File Upload Errors

###A TSV file was uploaded to the wrong index

If a TSV file is indexed in the wrong index, you must delete the shard that contains the new index data:

1. Determine the time range of the shard by running an IQL query for fields in the TSV file.
2. Sign into AWS and navigate to your S3 data bucket. 
3. Open the folder for the index that contains the new index data from the TSV file you uploaded.
4. From the list of compressed shards, locate the shard with the timestamp from your IQL query.
5. Delete the shard folder and corresponding document.

##Query Errors

 `=, :, !=, =~, !=~, (, *, \, %, +, -, /, >=, >, <=, <, in, not or not in expected, EOF encountered.`

You entered invalid text after a field name. Review the query syntax to ensure the query is not missing the operator (: or =). If the syntax is correct, add quotations around the field value. Example: country:”united states” <br><br>

`INTEGER, string literal or IDENTIFIER expected, EOF encountered.`

One of the fields has a typo.

[best-practices]: {{ site.baseurl }}/docs/best-practices
