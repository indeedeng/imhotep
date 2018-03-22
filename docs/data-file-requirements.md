---
layout: default
title: Data File Requirements
permalink: /docs/data-file-requirements/
---
## Table of Contents

* [Helper Script for Uploads](#helper-script-for-uploads)
* [Sample Data Files](#sample-data-files)
* [Filenames](#filenames)
* [Field Headers](#field-headers)
* [Field Values](#field-values)

<sub>Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc.go)</sub>

Each data file you create to upload to your dataset must follow requirements for field and header names and for field values. If you use gzip to compress your TSV file, you must use the `.tsv.gz` extension.

TSV Uploader supports parsing files that use either the TSV or CSV file format. 

*CSV Only:* If your files use the CSV file format, prepare the files to conform to the default settings for the [OpenCSV library](http://opencsv.sourceforge.net/apidocs/constant-values.html#au.com.bytecode.opencsv.CSVParser.DEFAULT_STRICT_QUOTES). OpenCSV uses \ (backslash) as the escape character.

## Helper Script for Uploads
We recommend using the [Imhotep Upload Format Helper Script](../imhotep_helper), which is a combination linter/converter (written in Python) that will make sure that your TSV or CSV data is formatted properly for upload to Imhotep.

## Sample Data Files
Use these sample data files as models for preparing your data for upload:<br>
[NASA Apache web logs](../sample-data#nasa-apache-web-logs)<br>
[Wikipedia web logs](../sample-data#wikipedia-web-logs)<br>
[World Cup 2014 player data](../sample-data#world-cup-2014-player-data)

## Filenames


### <strong><a name="shard-timerange"></a>Include the shard time range in the filename</strong> 
Imhotep partitions your data into shards by the time denoted in the filename. When you [specify a time range in IQL][timerange], Imhotep selects the shards to search based on the time period associated with each shard, not the [timestamps in the documents themselves](#time).

You can specify one full day, one full hour, or a range. If you specify a range, the end time is exclusive to the range.

The time denoted in the filename must be expressed in UTC-6. For example, for a file named `20140801.tsv`, the range of expected times in the file would be 2014-07-31 18:00:00 UTC (inclusive) to 2014-08-01 18:00:00 UTC (exclusive). Note that times in the file must be expressed as Unix timestamps (seconds or milliseconds since 1970).
<table>
  <tr>
    <th>Supported formats</th>
    <th>Example</th>
    <th>Description</th>
  </tr>
  <tr>
    <td valign="top">yyyyMMdd</td>
    <td valign="top"><code>20131201.tsv</code></td>
    <td valign="top">The file contains data for one day. </td>
  </tr>
   <tr>
    <td valign="top">yyyyMMdd.HH</td>
    <td valign="top"><code>20131201.01.tsv</code><br><code>20131201.02.tsv</code></td>
    <td valign="top">The file contains data for one hour of the day: 1-2 AM.<br>The file contains data for one hour of the day: 2-3 AM.</td>
  </tr>
  <tr>
    <td valign="top">yyyyMMdd.HH-yyyyMMdd.HH</td>
    <td valign="top"><code>20131201.00-20131201.03.tsv</code><br><code>20140901.00-20140903.00.tsv</code></td>
    <td valign="top">The file contains data for the first 3 hours of one day.<br>The file contains data for two full days.</td>
   </tr>
  </table>
  
### <strong>Optional prefixes and suffixes in the filename must be strings</strong>

You can add a string prefix or suffix, or both, to your filename. The builder ignores prefixes and suffixes that are strings. For example, the builder correctly ignores `QA_report` and `_combined` in the  `QA_report_20141013_combined.tsv` filename.

Digits are not supported in a prefix or suffix. For example, the `QA_report_20141013_parts1_2.tsv` filename is invalid because it includes digits in the suffix.

## Field Headers

### <strong>The first line of your file represents the header that defines fields in the resulting dataset</strong>

Use field names that contain uppercase `A-Z`, lowercase `a-z`, digits, or `_` (underscore). A field name cannot start with a digit.


### <strong><a name="time"></a>A document's timestamp must be in the same range as the filename</strong>

If the field name is `time` or `unixtime`, the builder parses that field’s values as Unix timestamps and uses them as the document’s timestamps in the dataset. A timestamp can be in seconds or milliseconds since Unix epoch time (UTC). If you don't include a `time` or `unixtime` field, the builder uses the start of the [shard time range](#shard-timerange) as the timestamp for all of the documents in the file.

NOTE: Do not use floating-point values in a `time` or `unixtime` field, because floating-point values are treated as strings. [Read more](#floating).

### <strong>Field names with the * suffix</strong>

Adding the `*` suffix to the field name in the header also indexes a tokenized version of that field. 
<table>
  <tr>
    <th>Field Name</th>
    <th>Value</th>
    <th>Indexed Values</th>
  </tr>
  <tr>
    <td valign="top">query*</td>
    <td valign="top">"project manager"</td>
    <td valign="top">query:"project manager"<br>querytok:"project"<br>querytok:"manager"</td>
  </tr>
 
</table>

### <strong>Field names with the ** suffix</strong>

Adding the `**` suffix to the field name in the header also indexes bigrams from the field value. 
<table>
  <tr>
    <th>Field Name</th>
    <th>Value</th>
    <th>Indexed Values</th>
  </tr>
  <tr>
    <td valign="top">query**</td>
    <td valign="top">"senior project manager"</td>
    <td valign="top">query:"senior project manager"<br>querytok:"senior"<br>querytok:"project"<br>querytok:"manager"<br>querybigram:"senior project"<br>querybigram:"project manager"</td>
  </tr> 
</table>

### <strong>Field names with the + suffix</strong>
Adding the `+` suffix to the field name in the header indexes the tokens in the field instead of the entire field. 
<table>
  <tr>
    <th>Field Name</th>
    <th>Value</th>
    <th>Indexed Values</th>
  </tr>
  <tr>
    <td valign="top">query_words+</td>
    <td valign="top">"project manager"</td>
    <td valign="top">query_words:"project"<br>query_words:"manager"</td>
  </tr>
 
</table>


## Field Values

### <strong>Prepare the values in your data file</strong>

Ensure that you remove tabs and newlines from your values.

*CSV Only:* Do not use quotations around field values. 

### <strong>Imhotep has two data types: string and integer(long)</strong>
For Imhotep to treat a field’s value as an integer, at least 90% of the values must be integers or blanks, and at least 20% of the total values must be valid integers.

Once set, a field's type must remain consistent. That is, once a field is indexed as an integer, the field must remain an integer. Likewise, if a field is indexed as a string, the field must remain a string. If a field's type is not the same in every shard for that dataset, data for one or the other type will not be accessible through IQL.

### <strong><a name="floating"></a>Floating-point values become strings</strong>

Floating-point values like 1.0 or 1.5 are not supported as integers and are treated as strings. To use floating-point values as metrics, consider the following methods:
<table>
  <tr>
    <th>Method</th>
    <th>Examples</th>
  </tr>
  <tr>
    <td valign="top">Round the values.</td>
    <td valign="top"><code>1.0 => 1</code><br><code>1.5 => 2</code></td>
   </tr> 
  <tr>
    <td valign="top">Multiply all values by a decimal constant.</td>
    <td valign="top"><code>1.0 => 10</code><br><code>1.5 => 15</code></td>
   </tr> 
</table>

### <strong>Empty values</strong>

An empty value for a string field is indexed as an empty string term and can be queried. For example, `location:""` returns the queries with an empty value for the string field `location`. For integer fields, all non-integer values including the empty value are not indexed and cannot be queried.

[timerange]: {{ site.baseurl }}/docs/timerange
[sample-data]: {{ site.baseurl }}/docs/sample-data
