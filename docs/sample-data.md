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

The dataset in [worldcupplayerinfo_20140701.tsv](). Each document in the dataset includes information about a single player. Consider the following fields in the dataset:

| | | |
| ----- | ------ | ------- |
| Player | String | Player’s name.
| Age | Int | Player’s age.
| Captain | Int | Value (1 or 0) indicates whether the player is a captain.
| Club | String | The player’s club when not playing for the national team in the World Cup.
| Country | String | The country the player represents in the World Cup.
| Group | String | The player’s national team belongs to this World Cup group.
| Jersey | Int | The player’s jersey number.
| Position | String | The player’s position.
| Rank | Int | The ranking of the country the player represents.
| Selections | Int | The number of World Cup appearances for this player.

Source: [Stack Exchange Network](http://opendata.stackexchange.com/questions/1791/any-open-data-sets-for-the-football-world-cup-in-brazil-2014) / Open Data<br>
The data are distributed under the creative commons [Attribution-Share Alike 4.0 International](http://creativecommons.org/licenses/by-sa/4.0/) license. The creator of the data is http://opendata.stackexchange.com/users/3061/bryan. In compliance with this license, the data is hereby attributed to the users and owners of StackOverflow, but not in such a way as to suggest that they endorse Indeed or Indeed’s use of the data.

