---
layout: default
title: Data Upload
permalink: /docs/data-upload/
---

Use TSV Uploader to make your data available in Imhotep.

## Prerequisites
Create data files to upload to your index. Each data file must follow these requirements for file and field names and field values.

### File Names
- Include the shard time range in the file name. Supported formats are yyyyMMdd, yyyyMMdd.HH, yyyyMMdd.HH-yyyyMMdd.HH. Example file names for a TSV file that includes data for December 12, 2013:
 - 20131201.tsv - file contains data for one day
 - 20131201.01.tsv - file contains data for 1-2 AM of the day
 - 20131201.00-20131201.03.tsv - file contains data for the first 3 hours of the day
- Do not use digits in an optional and arbitrary prefix or suffix. In the SQP_report_20131021_combined.tsv file, the builder ignores the digit and suffix.

### Field Headers
- Separate your fields with tabs.
- The first line of your file represents the header that defines fields in the resulting index. Use field names that match regex [A-Za-z_]+. Avoid using uppercase letters. Example: first {tab} last {tab} email  QUESTION: why include A-Z above if we recommend to not use uppercase?
- If the field name is time or unixtime, the builder parses that field’s values as Unix timestamps and uses them as the document’s timestamps in the index. A timestamp can be in seconds or milliseconds (since Unix epoch (UTC). QUESTION: what does this parenthetical mean and is it important for this doc?
- Adding the * suffix to the field name in the header also indexes that field in a tokenized version. For example, if a field name is q* with the value "project manager", the following values are indexed: q:"project manager", qtok:"project", qtok:"manager"
- Adding the ** suffix to the field name in the header also indexes that field in a bigram version. For example, if a field name is q** with the value "senior project manager", the following values are indexed: q:"senior project manager", qtok:"senior", qtok:"project", qtok:"manager", qbigram:"senior project", qbigram:"project manager"



