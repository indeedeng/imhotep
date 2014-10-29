---
layout: default
title: Sample Queries
permalink: /docs/sample-queries/
---
This page includes sample queries to run on our [demo cluster](http://demo.imhotep.works/iql/) preloaded with three sample datasets: `nasa`, `wikipedia` and `worldcup2014`. 

[Read about the sample data](../data-file-requirements#sample-data-files/).

## NASA 

[Hourly counts](http://demo.imhotep.works/iql/#q[]=from+nasa+%221995-06-30+22%3A00%3A00%22+%221995-09-02+00%3A00%3A00%22+group+by+time(1h)&view=graph) returns a graph of the full time range of the data set with the number of queries every hour. 

From the graph, the following insights are available:

- The graph shows no activity during [Hurricane Erin](http://en.wikipedia.org/wiki/Hurricane_Erin_(1995)).
- The peak hour of activity corresponds to the [Space Shuttle Discovery launch STS-70 on July 13](http://www.nasa.gov/mission_pages/shuttle/shuttlemissions/archives/sts-70.html). The [top 100 pages](http://demo.imhotep.works/iql/#q[]=from+nasa+%221995-07-13+07%3A00%3A00%22+%221995-07-13+08%3A00%3A00%22+group+by+url[100]&view=table) accessed during this peak hour were limited to the web covering shuttle liftoff.

[Popular non-images](http://demo.imhotep.works/iql/#q[]=from+nasa+%221995-07-01+00%3A00%3A00%22+%221995-09-01+00%3A00%3A00%22+where+url+!%3D~+%22.*gif%22+group+by+time(1d)%2C+url[1+by+count()]&view=table) lists the most popular non-image URLs on each day in the dataset.

## Wikipedia 

[Wikipedia articles that start with the letter E](http://demo.imhotep.works/iql/#q[]=from+wikipedia+%222014-09-13+11%3A00%3A00%22+%222014-09-13+12%3A00%3A00%22+where+title%3D~%22E.*%22+group+by+title[10+by+numRequests]+select+numRequests&view=table&table_sort[0][]=2&table_sort[0][]=desc) returns the names of the most popular Wikipedia articles that start with `E` from one hour on 9/13/2014.

## World Cup 2014

####<a name="captains"></a>Team Captains 

The following query returns the [average age](http://demo.imhotep.works/iql/#q[]=from+worldcup2014+2014-07-01+2014-07-02+group+by+Captain+select+Age%2Fcount()%2C+Selections%2Fcount()&view=table) of captains and players of all other positions. The query also compares the number of appearances in the World Cup for the two groups of players. Team captains are on average almost 5 years older than other players and have 3 times as many national team appearances.

<pre>from worldcup2014 2014-07-01 2014-07-02 group by Captain select Age/count(), Selections/count()</pre>

The following query [lists the captains](http://demo.imhotep.works/iql/#q[]=from+worldcup2014+2014-07-01+2014-07-02+where+Captain%3A1+group+by+Player%2C+Country[]%2C+Club[]%2C+Position[]+select+Selections&view=table&table_sort[0][]=5&table_sort[0][]=desc), along with their club, country, position, and number of World Cup appearances.

<pre>from worldcup2014 2014-07-01 2014-07-02 where Captain:1 group by Player, Country[], Club[], Position[] select Selections</pre>

####<a name="clubs"></a>Clubs

The following query returns [data for the top 25 clubs](http://demo.imhotep.works/iql/#q[]=from+worldcup2014+2014-07-01+2014-07-02+group+by+Club[25]+select+count()%2C+Captain%2C+Rank%2Fcount()%2C+Age%2Fcount()&view=table): number of players, number of captains, average country ranking of the team’s players, average player age. Barcelona has the most players in the World Cup (16), but Real Madrid/Man U have the most captains (2). Atletico Madrid has the highest average country rank for its players. Manchester City the oldest players, Schalke 4 the youngest.

<pre>from worldcup2014 2014-07-01 2014-07-02 group by Club[25] select count(), Captain, Rank/count(), Age/count()</pre>

####<a name="countries"></a>Countries

The following query returns [data by country](http://demo.imhotep.works/iql/#q[]=from+worldcup2014+2014-07-01+2014-07-02+group+by+Country+select+Age%2Fcount()%2C+Selections%2Fcount()&view=table&table_sort[0][]=2&table_sort[0][]=desc): average player age and average number of World Cup appearances. Argentina has the oldest team, Ghana the youngest. Spain is the most experienced, Australia the least.

<pre>from worldcup2014 2014-07-01 2014-07-02 group by Country select Age/count(), Selections/count()</pre>

####<a name="age-experience"></a>Age versus Experience

The following query [compares player age to the number of World Cup appearances](http://demo.imhotep.works/iql/#q[]=from+worldcup2014+2014-07-01+2014-07-02+group+by+Age+select+Selections%2Fcount()&view=pivot&table_sort[0][]=0&table_sort[0][]=asc&pivot_cols[]=Age&pivot_aggregator=Integer+Sum&pivot_renderer=Line+Chart). Not surprisingly, the older you are, the more appearances you've had, in general.

<pre>from worldcup2014 2014-07-01 2014-07-02 group by Age select Selections/count()</pre>

####<a name="jersey"></a>Jersey Numbers

The following query returns the [number of players grouped by their jersey number](http://demo.imhotep.works/iql/#q[]=from+worldcup2014+2014-07-01+2014-07-02+group+by+Jersey+select+count()%2C+Captain&view=table&table_sort[0][]=2&table_sort[0][]=desc). The query also returns the number of captains for each jersey number. Teams number all players 1-23. However, captains gravitate towards wearing #1, #4, #3, and #10.

<pre>from worldcup2014 2014-07-01 2014-07-02 group by Jersey select count(), Captain</pre>

The following query groups [documents by the player's jersey number](http://demo.imhotep.works/iql/#q[]=from+worldcup2014+2014-07-01+2014-07-02+group+by+Jersey%2C+Position[1]&view=table&table_sort[0][]=1&table_sort[0][]=asc) and then, for each jersey number group, returns the most common position for that jersey number. Some numbers are typically associated with a position: #1 is always the goalie, defenders are frequently #2 and #3, and #9 is usually a forward.

<pre>from worldcup2014 2014-07-01 2014-07-02 group by Jersey, Position[1]</pre>

####<a name="positions"></a>Positions

The following query returns the [average player age and average number of World Cup appearances by their position](http://demo.imhotep.works/iql/#q[]=from+worldcup2014+2014-07-01+2014-07-02+group+by+Position[4]+select+count()%2C+100*Captain%2Fcount()%2C+Age%2Fcount()%2C+Selections%2Fcount()&view=table&table_sort[0][]=2&table_sort[0][]=asc). Goalies are older and more frequently the captain. Forwards typically have the most experience.

<pre>from worldcup2014 2014-07-01 2014-07-02 group by Position select count(),100*Captain/count(), Age/count(), Selections/count()</pre>

####<a name="groups"></a>Groups

The following query returns data about the [World Cup groups](http://demo.imhotep.works/iql/#q[]=from+worldcup2014+2014-07-01+2014-07-02+group+by+Group+select+Selections%2Fcount()%2C+Age%2Fcount()%2C+Rank%2Fcount()&view=table&table_sort[0][]=3&table_sort[0][]=desc): average number of World Cup appearances, average age, and average country rank. Group D and G were rough. Group F and H were easy. Group H was also the youngest and least experienced, while group C was the oldest and most experienced.

<pre>from worldcup2014 2014-07-01 2014-07-02 group by Group select Selections/count(), Age/count(), Rank/count()</pre>
