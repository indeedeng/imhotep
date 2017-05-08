---
layout: default
title: Sample Queries
permalink: /docs/sample-queries/
---
This page includes sample queries to run on our [demo cluster](http://imhotep.indeed.tech/iql/) preloaded with three sample datasets: `apachejira`, `nasa`, `wikipedia` and `worldcup2014`. 

[Click here for information about the sample data](../sample-data/).

## apachejira

The following query shows the users who reported the most bugs in Apache Software Foundation projects:

<pre><code><a href="http://imhotep.indeed.tech/iql/q/78P8NE">from apachejira 2016-01-01 2017-05-01
   where action="create" issuetype="Bug"
   group by actor</a></code></pre>

The following query shows the 10 projects with the most reported bugs:

<pre><code><a href="http://imhotep.indeed.tech/iql/q/ENH6GG">from apachejira 2016-01-01 2017-05-01
   where action="create" issuetype="Bug"
   group by project[10]</a></code></pre>

The following query returns the number of unique contributors per project:

<pre><code><a href="http://imhotep.indeed.tech/iql/q/942RAY">from apachejira 2016-01-01 2017-05-01
   where status="Patch Available" fieldschangedtok="status"
   group by project
   select distinct(actor)</a></code></pre>
   
The following query shows the number of contributions per person in Hadoop Common, which is a very active project:

<pre><code><a href=" http://imhotep.indeed.tech/iql/q/WZ727K">from apachejira 2016-01-01 2017-05-01
   where status=”Patch Available”  fieldschangedtok=”status” project=”Hadoop Common”
   group by actor
   select distinct(issuekey)</a></code></pre>
   
From the graph (as of 2017-05-01), the following insights are available:

- There are 200 different people who have contributed a patch.
- Only 50 people have contributed 5 or more patches.
- On average, each person contributes 5 patches.


The following query shows the average number of hours it takes for a patch to be accepted per project:

<pre><code><a href="http://imhotep.indeed.tech/iql/q/XC7PGP">from apachejira 2016-01-01 2017-05-01
   where prevstatus="Patch Available" status="Resolved" fieldschangedtok="status"
   group by project
   select timesinceaction\3600/count()</a></code></pre>





## NASA 

The following query on hourly counts returns a graph of the full time range of the dataset with the number of queries every hour:

<pre><code><a href="http://imhotep.indeed.tech/iql/q/YRACA2">from nasa 1995-06-30 22:00:00 1995-09-02 00:00:00 
  group by time(1h)</a></code></pre>

From the graph, the following insights are available:

- The graph shows no activity during [Hurricane Erin](http://en.wikipedia.org/wiki/Hurricane_Erin_(1995)).
- The peak hour of activity corresponds to the [Space Shuttle Discovery launch STS-70 on July 13](http://www.nasa.gov/mission_pages/shuttle/shuttlemissions/archives/sts-70.html). 

The following query shows that the top 100 pages accessed during this peak hour were limited to shuttle liftoff coverage:

<pre><code><a href="http://imhotep.indeed.tech/iql/q/R86E6R">from nasa 1995-07-13 07:00:00 1995-07-13 08:00:00
  group by url[100]</a></code></pre>

This query lists the most popular non-image URLs on each day in the dataset:

<pre><code><a href="http://imhotep.indeed.tech/iql/q/XHKF46">from nasa 1995-07-01 00:00:00 1995-09-01 00:00:00 
  where url !=~ ".\*gif"
  group by time(1d), url[1 by count()]</a></code></pre>

## Wikipedia 

The following query returns the names of the most popular Wikipedia articles that start with `E` from one hour on 9/13/2014:

<pre><code><a href="http://imhotep.indeed.tech/iql/q/6Y8T2P">from wikipedia 2014-09-13 11:00:00 2014-09-13 12:00:00
  where title=~"E.\*"
  group by title[10 by numRequests]
  select numRequests</a></code></pre>

## World Cup 2014

### <a name="captains"></a>Team Captains 

The following query returns the average age of captains and players of all other positions. The query also compares the number of appearances in the World Cup for the two groups of players. Team captains are on average almost 5 years older than other players and have 3 times as many national team appearances.

<pre><code><a href="http://imhotep.indeed.tech/iql/q/674A2G">from worldcup2014 2014-07-01 2014-07-02 
  group by Captain 
  select Age/count(), Selections/count()]</a></code></pre>

The following query lists the captains, along with their club, country, position, and number of World Cup appearances.

<pre><code><a href="http://imhotep.indeed.tech/iql/q/Z79AKX">from worldcup2014 2014-07-01 2014-07-02
  where Captain:1 
  group by Player, Country[], Club[], Position[] 
  select Selections</a></code></pre>

### <a name="clubs"></a>Clubs

The following query returns data for the top 25 clubs: number of players, number of captains, average country ranking of the team’s players, average player age. Barcelona has the most players in the World Cup (16), but Real Madrid/Man U have the most captains (2). Atletico Madrid has the highest average country rank for its players. Manchester City the oldest players, Schalke 4 the youngest.

<pre><code><a href="http://imhotep.indeed.tech/iql/q/NH2ZK4">from worldcup2014 2014-07-01 2014-07-02
  group by Club[25] 
  select count(), Captain, Rank/count(), Age/count()</a></code></pre>

### <a name="countries"></a>Countries

The following query returns data by country: average player age and average number of World Cup appearances. Argentina has the oldest team, Ghana the youngest. Spain is the most experienced, Australia the least.

<pre><code><a href="http://imhotep.indeed.tech/iql/q/AYFHDC">from worldcup2014 2014-07-01 2014-07-02 
  group by Country 
  select Age/count(), Selections/count()</a></code></pre>

### <a name="age-experience"></a>Age versus Experience

The following query compares player age to the number of World Cup appearances. Not surprisingly, the older you are, the more appearances you've had, in general.

<pre><code><a href="http://imhotep.indeed.tech/iql/q/4M3HHX">from worldcup2014 2014-07-01 2014-07-02 
  group by Age
  select Selections/count()</a></code></pre>

### <a name="jersey"></a>Jersey Numbers

The following query returns the number of players grouped by their jersey number. The query also returns the number of captains for each jersey number. Teams number all players 1-23. However, captains gravitate towards wearing #1, #4, #3, and #10.

<pre><code><a href="http://imhotep.indeed.tech/iql/q/DWFWPD">from worldcup2014 2014-07-01 2014-07-02 
  group by Jersey 
  select count(), Captain</a></code></pre>

The following query groups documents by the player's jersey number and then, for each jersey number group, returns the most common position for that jersey number. Some numbers are typically associated with a position: #1 is always the goalie, defenders are frequently #2 and #3, and #9 is usually a forward.

<pre><code><a href="http://imhotep.indeed.tech/iql/q/6GE6K3">from worldcup2014 2014-07-01 2014-07-02 
  group by Jersey, Position[1]</a></code></pre>

### <a name="positions"></a>Positions

The following query returns the average player age and average number of World Cup appearances by their position. Goalies are older and more frequently the captain. Forwards typically have the most experience.

<pre><code><a href="http://imhotep.indeed.tech/iql/q/8PMZPK">from worldcup2014 2014-07-01 2014-07-02
  group by Position 
  select count(), 100\*Captain/count(), Age/count(), Selections/count()</a></code></pre>

### <a name="groups"></a>Groups

The following query returns data about the World Cup groups: average number of World Cup appearances, average age, and average country rank. Group D and G were rough. Group F and H were easy. Group H was also the youngest and least experienced, while group C was the oldest and most experienced.

<pre><code><a href="http://imhotep.indeed.tech/iql/q/NRTEE9">from worldcup2014 2014-07-01 2014-07-02 
  group by Group 
  select Selections/count(), Age/count(), Rank/count()</a></code></pre>
