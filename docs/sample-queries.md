---
layout: default
title: Sample Queries
permalink: /docs/sample-queries/
---

Because IQL allows you to join multiple result tables from multiple IQL queries, you can see data from multiple datasets and time ranges or filtered in different ways at one time. For example, at Indeed, we use Imhotep to answer these and many more questions about how people around the world are using our job search engine:

- How many unique job queries were performed on a specific day in a specific country?
- What are the top 50 queries in a specific country? How many times did job seekers click on a search result for each of those queries?
- Which job titles have the highest click-through rate for the query `Architecture` in the US? Which titles have the lowest click-through rate?

NOTE: Multiple queries should produce a result set that is consistent (has the same columns) and can be joined meaningfully.


##World Cup 2014 Player Data

The following queries all use the sample `worldcup2014` dataset and return data for `2014-07-01` to `2014-07-02`. 

Queries are grouped as follows:<br>
[Team Captains](#team-captains)<br>
[Clubs](#clubs)<br>
[Countries](#countries)<br>
[Age v Experience](#age-experience)<br>
[Jersey Numbers](#jersey)<br>
[Positions](#positions)<br>
[Groups](#groups)<br>

Since this is not typical time-series Imhotep data, all documents are assigned the same timestamp: `2014-07-01 00:00:00`

####<a name="captains"></a>Team Captains 

The following query returns the average age of captains and players of all other positions. The query also compares the number of appearances in the World Cup for the two groups of players. Team captains are on average almost 5 years older than other players and have 3 times as many national team appearances.

<pre>from worldcup2014 2014-07-01 2014-07-02 group by Captain select Age/count(), Selections/count()</pre>

![Average Ages](http://indeedeng.github.io/imhotep/images/team_captains_1.jpeg?raw=true)

The following query lists the captains, along with their club, country, position, and number of World Cup appearances.

<pre>from worldcup2014 2014-07-01 2014-07-02 where Captain:1 group by Player, Country[], Club[], Position[] select Selections</pre>

![Average Ages](http://indeedeng.github.io/imhotep/images/team_captains_2.jpeg?raw=true)

####<a name="clubs"></a>Clubs

The following query returns data for the top 25 clubs: number of players, number of captains, average country ranking of the team’s players, average player age. Barcelona has the most players in the World Cup (16), but Real Madrid/Man U have the most captains (2). Atletico Madrid has the highest average country rank for its players. Manchester City the oldest players, Schalke 4 the youngest.

<pre>from worldcup2014 2014-07-01 2014-07-02 group by Club[25] select count(), Captain, Rank/count(), Age/count()</pre>

![Average Ages](http://indeedeng.github.io/imhotep/images/clubs.jpeg?raw=true)

####<a name="countries"></a>Countries

The following query returns data by country: average player age and average number of World Cup appearances. Argentina has the oldest team, Ghana the youngest. Spain is the most experienced, Australia the least.

<pre>from worldcup2014 2014-07-01 2014-07-02 group by Country select Age/count(), Selections/count()</pre>

![Average Ages](http://indeedeng.github.io/imhotep/images/countries.jpeg?raw=true)

####<a name="age-experience"></a>Age versus Experience

The following query compares player age to the number of World Cup appearances. Not surprisingly, the older you are, the more appearances you've had, in general.

<pre>from worldcup2014 2014-07-01 2014-07-02 group by Age select Selections/count()</pre>

![Average Ages](http://indeedeng.github.io/imhotep/images/age_vs_experience.jpeg?raw=true)

####<a name="jersey"></a>Jersey Numbers

The following query returns the number of players grouped by their jersey number. The query also returns the number of captains for each jersey number. Teams number all players 1-23. However, captains gravitate towards wearing #1, #4, #3, and #10.

<pre>from worldcup2014 2014-07-01 2014-07-02 group by Jersey select count(), Captain</pre>

![Average Ages](http://indeedeng.github.io/imhotep/images/jersey_number_1.jpeg?raw=true)

The following query groups documents by the player's jersey number and then, for each jersey number group, returns the most common position for that jersey number. Some numbers are typically associated with a position: #1 is always the goalie, defenders are frequently #2 and #3, and #9 is usually a forward.

<pre>from worldcup2014 2014-07-01 2014-07-02 group by Jersey, Position[1]</pre>

![Average Ages](http://indeedeng.github.io/imhotep/images/jersey_number_2.jpeg?raw=true)

####<a name="positions"></a>Positions

The following query returns the average player age and average number of World Cup appearances by their position. Goalies are older and more frequently the captain. Forwards typically have the most experience.

<pre>from worldcup2014 2014-07-01 2014-07-02 group by Position select count(),100*Captain/count(), Age/count(), Selections/count()</pre>

![Average Ages](http://indeedeng.github.io/imhotep/images/positions.jpeg?raw=true)

####<a name="groups"></a>Groups

The following query returns data about the World Cup groups: average number of World Cup appearances, average age, and average country rank. Group D and G were rough. Group F and H were easy. Group H was also the youngest and least experienced, while group C was the oldest and most experienced.

<pre>from worldcup2014 2014-07-01 2014-07-02 group by Group select Selections/count(), Age/count(), Rank/count()</pre>

![Average Ages](http://indeedeng.github.io/imhotep/images/groups.jpeg?raw=true)