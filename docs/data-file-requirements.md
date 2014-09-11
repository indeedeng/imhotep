---
layout: default
title: Data File Requirements
permalink: /docs/data-file-requirements/
---

Create tab-separated data files to upload to your index. Each data file must follow requirements for field and header names and for field values.

## Filenames

#### Include the shard time range in the filename

| Supported formats | Example | Description |
|------ | ------ | --------- |
| yyyyMMdd | `20131201.tsv` | The file contains data for one day. |
| yyyyMMdd.HH | `20131201.01.tsv` | The file contains data for 1-2 AM of the day. |
| yyyyMMdd.HH-yyyyMMdd.HH | `20131201.00-20131201.03.tsv` | The file contains data for the first 3 hours of the day.  |


#### Do not use digits in an arbitrary prefix or suffix 

If you include a prefix or suffix in the filename, do not use integers. For example, the builder ignores the prefix and suffix in the `SQP_report_20131021_combined.tsv` filename. 

## Field Headers

#### The first line of your file represents the header that defines fields in the resulting index 

Use field names that match regex [A-Za-z_]+. Avoid using uppercase letters. For example:  
`first {tab} last {tab} email`

QUESTION: why include A-Z above if we recommend to not use uppercase?

#### time or unixtime field names

If the field name is time or unixtime, the builder parses that field’s values as Unix timestamps and uses them as the document’s timestamps in the index. A timestamp can be in seconds or milliseconds (since Unix epoch (UTC). QUESTION: what does this parenthetical mean and is it important for this doc?

#### Field names with the * suffix

Adding the * suffix to the field name in the header also indexes that field in a tokenized version. For example, if a field name is q* with the value "project manager", the following values are indexed: q:"project manager", qtok:"project", qtok:"manager"

| Field Name | Value | Indexed Values |
| ------ | --------- | ---------- |
| q* | "project manager" | q:"project manager"|
| | | qtok:"project" | 
| | | qtok:"manager" | 


#### Field names with the ** suffix

Adding the ** suffix to the field name in the header also indexes that field in a bigram version. 

| Field Name | Value | Indexed Values |
| ------ | --------- | ---------- |
| q** | "senior project manager" | q:"senior project manager"|
| | | qtok:"senior" | 
| | | qtok:"project" | 
| | | qtok:"manager" | 
| | | qbigram:"senior project" | 
| | | qbigram:"project manager" | 
 

## Field Values

#### Prepare the values in your data file

Do not use quotations around field values. Ensure that you remove tabs and newlines from your values.

#### Floating-point values become strings

Floating-point values like 1.0 or 1.5 are not supported as integers and are treated as strings. To use floating-point values as metrics, consider the following methods:

| Method | Example |
| ------ | ---------- |
| Round the values. | 1.0 => 1.5, 1.5 => 1 |
| Multiply all values by a decimal constant. | 1.0 => 10, 1.5 => 15 |

#### Empty values

An empty value for a string field is indexed as an empty string term and can be queried. For integer fields, all non-integer values including the empty value are not indexed and cannot be queried.


