---
layout: default
title: Computing Metrics
permalink: /docs/metrics/
---

Use the optional **select** filter to compute metrics for your aggregated groups. Use these rules when constructing the **select** statement:

- Separate metric statements or distinct() function calls by commas.
- If you leave this control empty, IQL returns a count of all documents in each group. 

The following filters are available:

| Filter | Syntax | Example |
| --------- | ------------- | ---------|
| Simple metric name | DESCRIBE indexName |  |
| Arithmetic expression of two metrics (+, -, *, %) executed on each document. The divide operation (/) is executed in aggregate after everything else is computed. If you require a per document divide, use a reverse slash (/). |  |  |
| Function calls | count() |  |
|  | cached(metric...) |  |
|  | exp(...) |  |
|  | dynamic(metric…) |  | 
|  | hasstr(field, term) |  | 
|  | hasstr(“field:term”) |  | 
|  | hasint(field, term) |  | 
|  | hasint(“field:term”) |  | 
|  | floatscale(...) |  | 
| distinct() | distinct(field) | `distinct(country)` returns a count of distinct terms for the country in each grouping. |
| percentile() | percentile(field, N) | `percentile(totaltime, 50)` returns the median value of `totaltime`. |



