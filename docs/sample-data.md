---
layout: default
title: Sample Data
permalink: /docs/sample-data/
---
## Page Contents

* [Apache Software Foundation Issue Tracker](#apache-software-foundation-issue-tracker)
* [NASA Apache Web Logs](#nasa-apache-web-logs)
* [Wikipedia Web Logs](#wikipedia-web-logs)
* [World Cup 2014 Player Data](#world-cup-2014-player-data)


<sub>Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc.go)</sub>

## Apache Software Foundation Issue Tracker
The sample time-series dataset in apachejira comes from [public Apache instance of JIRA](https://issues.apache.org/jira/). It was converted to a TSV file with a Java program that uses the JIRA API, with the following fields:


| | |
| ----- | ------- |
| action | The action taken, one of “comment”, “create”, or “update”. |
| actor | The name of the user who took the action. |
| assignee | The name of the user to which this issue is assigned. |
| category | The category to which the issue belongs. Multiple projects roll up to a single category. |
| fieldschanged | Which fields were modified in this action. A single field containing a space-separated list of fields modified. For example, if the action changed the assignee and status in this action, there will be one result: “assignee status”. More appropriate for display. |
| fieldschangedtok | Which fields were modified in this action. A multi-valued field containing one result for each field changed. For example, if the action changed the assignee and status in this action, there are two different results: “assignee” and “status”. More appropriate for filtering. |
| fixversion | The fixversions this issue was set to when this action was taken. A single field containing a pipe (&#124;) separated list of fixversions. For example, if this issue is assigned to fixversions master and 7.0, there is one result: "master&#124;7.0". More appropriate for display. |
| fixversiontok | The fixversions this issue was set to when this action was taken. A multi-valued field containing one fix version each. For example, if this issue is assigned to fixversions master and 7.0, there are two results: “master” and “7.0”. More appropriate for filtering. |
| issueage | The number of seconds between when this action took place and when the issue was created. |
| issuekey | The key of the specified JIRA ticket. |
| issuetype | The type of this ticket. |
| prevstatus | The status of the ticket prior to this action. If the action did not change status, this will always be the same as the current status. |
| project | The name of the project (not the abbreviation) to which this issue belongs. |
| reporter | The name of the person that created this issue. |
| resolution | If this issue is resolved, the string value of the Resolution field. Otherwise blank. |
| status | The status of the issue. |
| summary | he summary (i.e., short description or name) of the ticket. |
| timeinstate | The number of seconds between when this action took place and when the last action changed this issue’s status. |
| timesinceaction | The number of seconds between this action and the last action. |
| unixtime | The unix timestamp for this action. |

Adapted from: [Apache Software Foundation JIRA](http://issues.apache.org/jira/)



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

| TSV Data Size (raw uncompressed) | Imhotep Data Size |
| ----- | ------- |
| 256 MB | 19 MB |

Source: [Internet Traffic Archive](http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html)

## Wikipedia Web Logs
The time-series data in [wikipedia_e_20140913.11.tsv.gz](http://indeedeng.github.io/imhotep/files/wikipedia_e_20140913.11.tsv.gz) is one hour of data from 9/13/2014 for Wikipedia articles beginning with the letter E. 

Each document corresponds to a Wikipedia article that was served in that hour:

| | |
| ----- | ------- |
| title | Title of the article on Wikipedia
| categories+ | List of categories in which the article is contained
| titleWords+ | List of words in the title
| linksOut+ | List of Wikipedia articles linked by the article
| numRequests | Number of requests for the article in that hour
| bytesServed | Number of bytes served for the article in that hour

[The most popular E entry](../../images/wikipedia2.png){:target="_blank" rel="noopener"} in that hour was `English_alphabet`.

| title | categories+ | titleWords+ | linksOut+ | numRequests | bytesServed | 
| ----- | ----- | ----- | ----- | ----- | ----- | ----- |
| <span class="smallcode">`English_alphabet`</span> | <span class="xscode">`All_Wikipedia_articles_needing_clarification All_articles_needing_additional_references All_articles_with_unsourced_statements Articles_containing_Old_English-language_text Articles_needing_additional_references_from_June_2011 Articles_with_hAudio_microformats Articles_with_unsourced_statements_from_January_2011 Articles_with_unsourced_statements_from_July_2010 Articles_with_unsourced_statements_from_March_2014 English_spelling Latin_alphabets Wikipedia_articles_needing_clarification_from_August_2013`</span> | <span class="smallcode">`English alphabet`</span> |  <span class="xscode"><a href="../../images/wikipedia3.png" target="_blank" rel="noopener">`A Adjective Aircraft Alphabet_song American_English American_braille American_manual_alphabet Ampersand Anglo-Saxon_futhorc Anglo-Saxons Ansuz_(rune) Apostrophe B Body_cavity British_English Byrhtfert ...`</a></span> | <span class="smallcode">`960`</span> | <span class="smallcode">`21124206`</span> |


| TSV Data Size (raw uncompressed) | Imhotep Data Size |
| ----- | ------- |
| 2450 GB | 272 GB |

Source: https://dumps.wikimedia.org/other/pagecounts-raw/ for page counts and https://dumps.wikimedia.org/backup-index.html for all other fields

## World Cup 2014 Player Data

The dataset in [worldcupplayerinfo_20140701.tsv](http://indeedeng.github.io/imhotep/files/worldcupplayerinfo_20140701.tsv) includes information about players in the World Cup 2014. Since this is not typical time-series Imhotep data, all documents are assigned the same timestamp: `2014-07-01 00:00:00`

Each document in the dataset includes information about a single player:

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

| TSV Data Size (raw uncompressed) | Imhotep Data Size |
| ----- | ------- |
| 45 KB | 15 KB |

Source: [Stack Exchange Network](http://opendata.stackexchange.com/questions/1791/any-open-data-sets-for-the-football-world-cup-in-brazil-2014) / Open Data<br>
The data are distributed under the creative commons [Attribution-Share Alike 4.0 International](http://creativecommons.org/licenses/by-sa/4.0/) license. The creator of the data is http://opendata.stackexchange.com/users/3061/bryan. In compliance with this license, the data is hereby attributed to the users and owners of StackOverflow, but not in such a way as to suggest that they endorse Indeed or Indeed’s use of the data.

