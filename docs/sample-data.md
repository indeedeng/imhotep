---
layout: default
title: Sample Data
permalink: /docs/sample-data/
---

## NASA Apache Web Logs
The sample time-series dataset in [nasa_19950801.tsv](http://indeedeng.github.io/imhotep/files/nasa_19950801.tsv) comes from [public 1995 NASA Apache web logs](http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html). The file contains data for a single day and is in an Imhotep-friendly TSV format.

A Perl script was used to convert the Apache web log into the TSV format, extracting the following fields:

| | |
| ----- | ------- |
| host | When possible, the hostname making the request. Uses the IP address if the hostname was unavailable. |
| logname | Unused, always `-` |
| time | In seconds, since 1970 |
| method | HTTP method: GET, HEAD, or POST |
| url | Requested path |
| response | HTTP response code |
| bytes | Number of bytes in the reply |

Here is an example line (or document) from the dataset:

<pre>piweba3y.prodigy.com - 807301196 GET /shuttle/missions/missions.html 200 8677</pre>

The timestamp `807301196` is the conversion of `01/Aug/1995:13:19:56 -0500` using Perl:

<pre>use Date::Parse;
$in = "01/Aug/1995:13:19:56 -0500";
$out = str2time($in);
print "$out\n";</pre>

Data for two months are available in these compressed files:<br>
[nasa_19950630.22-19950728.12.tsv.gz](http://indeedeng.github.io/imhotep/files/nasa_19950630.22-19950728.12.tsv.gz)<br>
[nasa_19950731.22-19950831.22.tsv.gz](http://indeedeng.github.io/imhotep/files/nasa_19950731.22-19950831.22.tsv.gz)

## Wikipedia Web Logs

## World Cup 2014 Data
