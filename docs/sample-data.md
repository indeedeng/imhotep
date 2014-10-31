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

[The most popular E entry](http://54.214.252.202/iql/#q[]=from+wikipedia+%222014-09-13+11%3A00%3A00%22+%222014-09-13+12%3A00%3A00%22+where+title%3D~%22E.*%22+group+by+title[10+by+numRequests]+select+numRequests&view=table&table_sort[0][]=2&table_sort[0][]=desc) in that hour was `English_alphabet`.

| title | categories+ | titleWords+ | linksOut+ | numRequests | bytesServed | 
| ----- | ----- | ----- | ----- | ----- | ----- | ----- |
| <span class="smallcode">`English_alphabet`</span> | <span class="xscode">`All_Wikipedia_articles_needing_clarification All_articles_needing_additional_references All_articles_with_unsourced_statements Articles_containing_Old_English-language_text Articles_needing_additional_references_from_June_2011 Articles_with_hAudio_microformats Articles_with_unsourced_statements_from_January_2011 Articles_with_unsourced_statements_from_July_2010 Articles_with_unsourced_statements_from_March_2014 English_spelling Latin_alphabets Wikipedia_articles_needing_clarification_from_August_2013`</span> | <span class="smallcode">`English alphabet`</span> |  <span class="xscode"><a href="http://demo.imhotep.works/iql/#q[]=from+wikipedia+%222014-09-13+11%3A00%3A00%22+%222014-09-13+12%3A00%3A00%22+where+title%3D%22English_alphabet%22+group+by+linksOut&view=table&table_sort[0][]=2&table_sort[0][]=desc">`A Adjective Aircraft Alphabet_song American_English American_braille American_manual_alphabet Ampersand Anglo-Saxon_futhorc Anglo-Saxons Ansuz_(rune) Apostrophe B Body_cavity British_English Byrhtfert ...`</a></span> | <span class="smallcode">`960`</span> | <span class="smallcode">`21124206`</span> |


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

