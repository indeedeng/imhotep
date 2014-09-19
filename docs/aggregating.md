---
layout: default
title: Aggregating Results
permalink: /docs/aggregating/
---

Use the optional **group by** filter to group documents and retrieve aggregated stats. Separate each filter from another with a comma. If you leave this control empty, IQL places all documents into a single group and returns one row.

The following filters are available:

| Filter | Syntax | Example |
| --------- | ------------- | ---------|
| Simple grouping by field name | field name | `country` |
| Limit the number of groups (top/bottom K). | field[number] | `country[5]` returns the top 5 countries by count. |
|  | field[bottomNumber by metric] | `country[bottom 5 by clicks]` specifies the metric by which to order and uses the bottom countries instead of the top. |
| Exclude results for fields in which your specified metrics equal 0. | field[] | `country[]` returns results for all countries with metrics that are not zero. `country, group[]` returns results for groups that exist in each country. `country, group` returns a full cross product of countries and groups, including groups for countries where the group is not present and all metrics are 0. |
| Group your data into buckets by ranges you define. This construction automatically determines the size of the buckets and is useful for graphs. The values for min, min2, max, max2, interval and interval2 are numbers. | buckets(metric, min, max, interval | `buckets(accountbalance, 10, 100, 20)` |
| For multiple group-bys with buckets, include all bucket definitions in one statement. | buckets(metricX, min, max, interval metricY, min2, max2, interval2) | `buckets(time(1d), 1, 10, 1, accountbalance 10, 100, 20)` |
| Group your data into time buckets. The bucket size uses the same syntax as the relative values for the start and end values in the **timerange** filter: s, m, h, d or w. | time(bucketSize) | `time(1h)` groups data into buckets, each of which includes data from 1 hour. |
| You can also define the bucket size as an absolute value. | Nb | `time(3b)` groups data into 3 buckets, each of which includes data from one-third of the given time range. |
| IN construction for including more than one term. Using the IN construction in the **group by** filter is the same as using the IN construction in the **where** filter and then grouping by field name. | field in (term,term) | country in (canada,us) |
|  | field in ("term",term) | `country in ("great britain",deutschland)` |
|  | field not in (term,term) | `country not in (france,canada)` |
