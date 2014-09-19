---
layout: default
title: Limiting the Query
permalink: /docs/limiting/
---

Use the optional **where** filter to limit the query to only those documents that match the criteria you specify. Fields that are available are appropriate to the index you selected in **from**. 

Use these rules when constructing the **where** statement:

- Separate filters by a space or `and`.
- To negate a filter, precede the definition with `-` (minus sign).
- If you leave this control empty, IQL considers all documents. 

The following filters are available:

| Filter | Syntax | Example |
| --------- | ------------- | ---------|
| Field/term pairs | field=term | `country=greatbritain` |
|  | field="term" | `field="great britain"`
|  | field:term | `country:japan` |
|  | field!=term | `country!=us` |
| Regular expressions | field=~regex | `qnorm=~".*online marketing.*"` returns the top queries that contain the substring `online marketing`. IQL uses java 7 syntax, referenced here: http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html |
| Metric/integer pairs. A metric is a field that represents an integer value. | metric=integer | `count=100` |
|  | metric!=integer |  |
|  | metric<integer |  |
|  | metric<=integer |  |
|  | metric>integer |  |
|  | metric>=integer |  |
| IN construction for including more than one term. | field in (term,term) | `country in (greatbritain,france)` |
|  | field in ("term",term) | `country in ("great britain",france)` |
|  | field not in (term,term) | `country not in (canada,us,germany)` |
| The lucene () function that allows you to perform a logical OR of conditions on different fields or filter by a range of strings like field:[a TO b] | lucene("luceneQueryStr") | `lucene("(-resultA:0) OR (-resultB:0)")` returns the number of documents in the index that result in at least one `resultA` or one `resultB`. |
| The sample() function allows you to retain a portion of the documents. The sampling denominator is 100 if you don't specify a value.   | sample(field, samplingRatioNumerator, [samplingRatioDenominator=100]) | `sample(accountid, 1)` returns 1% of account IDs. `sample(accountid, 1, 1000)` returns .1% of account IDs. |
| By default, rerunning the query retrieves a different set of documents. Use a custom seed to retrieve the same documents when you rerun the query. | sample(field, samplingRatioNumerator, [samplingRatioDenominator=100], [randomSeed]) |  |


