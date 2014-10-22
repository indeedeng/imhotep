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

##Example: World Cup 2014 Data

The following sample queries are based on World Cup 2014 data from an example `worldcup2014` dataset. Each document in the dataset includes information about a single player. Consider the following fields in the dataset:

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
| Rank | Int | The player’s ranking.
| Selections | Int | The number of World Cup appearances for this player.

###Queries

The following queries all use our example `worldcup2014` dataset and return data for `2014-07-01` to `2014-07-02`.

####Team Captains

`from worldcup2014 2014-07-01 2014-07-02 group by Captain select Age/count(), Selections/count()`

Returns the average age of captains and players of all other positions. The query also compares the number of appearances in the World Cup for the two groups of players.

`from worldcup2014 2014-07-01 2014-07-02 where Captain:1 group by Player, Country[], Club[], Position[] select Selections`

Lists the captains, along with their club, country, position, and number of World Cup appearances.

####Clubs

`from worldcup2014 2014-07-01 2014-07-02 group by topterms(Club, 25) select count(), Captain, Rank/count(), Age/count()`

Returns data for the top 25 clubs: number of players, number of captains, average ranking, average age.

####Countries

`from worldcup2014 2014-07-01 2014-07-02 group by Country select Age/count(), Selections/count()`

Returns data by country: average player age and average number of World Cup appearances.

####Age versus Experience

`from worldcup2014 2014-07-01 2014-07-02 group by Age select Selections/count()`

Compares player age to the number of World Cup appearances.

####Jersey Number

`from worldcup2014 2014-07-01 2014-07-02 group by Jersey select count(), Captain`

Returns the number of players grouped by their jersey number. The query also returns the number of captains for each jersey number.

`from worldcup2014 2014-07-01 2014-07-02 group by Jersey, topterms(Position,1)`

Lists the player positions by jersey number.

####Positions

`from worldcup2014 2014-07-01 2014-07-02 group by Position select count(),100*Captain/count(), Age/count(), Selections/count()`

Returns the average player age and average number of World Cup appearances by their position.

####Groups

`from worldcup2014 2014-07-01 2014-07-02 group by Group select Selections/count(), Age/count(), Rank/count()`

Returns data about the World Cup groups: average number of World Cup appearances, average age, and average player rank.