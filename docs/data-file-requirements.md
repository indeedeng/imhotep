---
layout: default
title: Data File Requirements
permalink: /docs/data-file-requirements/
---

Create tab-separated data files to upload to your index. Each data file must follow requirements for field and header names and for field values. If you use gzip to compress your TSV file, you must use the `.tsv.gz` extension.

## Filenames

#### Include the shard time range in the filename

| Supported formats | Example | Description |
|------ | ------ | --------- |
| yyyyMMdd | `20131201.tsv` | The file contains data for one day. |
| yyyyMMdd.HH | `20131201.01.tsv` | The file contains data for 1-2 AM of the day. |
| yyyyMMdd.HH-yyyyMMdd.HH | `20131201.00-20131201.03.tsv` | The file contains data for the first 3 hours of the day.  |


#### Do not use digits in an arbitrary prefix or suffix 

If you include a prefix or suffix in the filename, do not use integers. For example, the builder ignores the prefix and suffix in the `QA_report_20131021_combined.tsv` filename. 

## Field Headers

#### The first line of your file represents the header that defines fields in the resulting index 

Use field names that match regex [A-Za-z_]+. 


#### time or unixtime field names

If the field name is time or unixtime, the builder parses that field’s values as Unix timestamps and uses them as the document’s timestamps in the index. A timestamp can be in seconds or milliseconds since Unix epoch time (UTC). If you use time, the 32-bit number represents the number of seconds since January 1, 1970. If you use unixtime, the 64-bit number represents the number of milliseconds since January 1, 1970.

#### Field names with the * suffix

Adding the * suffix to the field name in the header also indexes that field in a tokenized version. For example, if a field name is q* with the value "project manager", the following values are indexed: query:"project manager", querytoken:"project", querytoken:"manager"

| Field Name | Value | Indexed Values |
| ------ | --------- | ---------- |
| query* | "project manager" | query:"project manager"|
| | | querytoken:"project" | 
| | | querytoken:"manager" | 


#### Field names with the ** suffix

Adding the ** suffix to the field name in the header also indexes that field in a bigram version. 

| Field Name | Value | Indexed Values |
| ------ | --------- | ---------- |
| query** | "senior project manager" | query:"senior project manager"|
| | | querytoken:"senior" | 
| | | querytoken:"project" | 
| | | querytoken:"manager" | 
| | | querybigram:"senior project" | 
| | | querybigram:"project manager" | 
 

## Field Values

#### Prepare the values in your data file

Do not use quotations around field values. Ensure that you remove tabs and newlines from your values.

####Imhotep has 2 data types: string and integer(long)
For Imhotep to treat a field’s value as an integer, at least 90% of the values must be integers or blanks, and at least 20% of the total values must be valid integers.

Once a field is indexed as an integer, it is always an integer.

#### Floating-point values become strings

Floating-point values like 1.0 or 1.5 are not supported as integers and are treated as strings. To use floating-point values as metrics, consider the following methods:

| Method | Example |
| ------ | ---------- |
| Round the values. | 1.0 => 1.5, 1.5 => 1 |
| Multiply all values by a decimal constant. | 1.0 => 10, 1.5 => 15 |

#### Empty values

An empty value for a string field is indexed as an empty string term and can be queried. For integer fields, all non-integer values including the empty value are not indexed and cannot be queried.


