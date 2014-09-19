---
layout: default
title: Best Practices
permalink: /docs/best-practices/
---

Use this section to avoid overloading the system by running heavy queries.

####Test on a small time range
Start small and then ramp up to the required range if performance is sufficient. 

| Use |  Do not use |
| ------ | --------|
| `timerange 1h today` | `timerange 180d today` |

####Determine the actual number of expected groups
If you think your query will return a large number of groups, run a DISTINCT query to return the actual number of expected groups before grouping your data:

`timerange 1h today select distinct(accountid)`

If the number of expected groups is a value that your system can handle, run the **group by** query:

`timerange 1h today group by accountid`

####Make the largest grouping the last
If ascending order on all columns from left to right is not necessary, try making the largest grouping the last grouping and make it non-exploded by adding square brackets to the field name. This allows the result to be streamed instead of stored in memory.

| Use | Do not use |
| ------ | -------- |
| `group by country, q[]` |  `group by country, q` |
|  | `group by q, country[]` |
|  | `group by q[500000], country[50]` |

The `group by q[500000], country[50]` is especially problematic because IQL can’t verify in advance how many terms will be returned. If the requested number is too high, IQL uses too much memory and requires time to recover.

####Avoid using DISTINCT for large queries
Don’t use distinct() as a metric with a large amount of data if you are using the **group by** filter with a large amount of data. 

#### Ask yourself: Could a standard database program handle the query?
If a standard application like Microsoft Excel cannot handle the resulting data from your query, your data will most likely also overwhelm IQL unless you carefully structure the query as described in the preceding performance considerations.
