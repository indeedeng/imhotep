---
layout: default
title: Overview
permalink: /docs/overview/
---

IQL is a query language for retrieving your data that is stored in Imhotep. To launch IQL, open a browser and navigate to the IQL URL provided from the AWS configuration. The interface includes the following controls:

| Control | Description |
| --------- | ------------- |
| **from** | The name of the index you created in Imhotep TSV Uploader. You cannot edit the list of indexes in IQL. |
| **timerange** | The required time range filter. [Read more][timerange]. | 
| **where** | Specifies which documents to include in the query. Fields that are available are appropriate to the index you selected in from. If you leave this control empty, IQL considers all documents. [Read more][limiting].  |
| **group by** | Specifies how to group the documents so that you can retrieve aggregated stats. If you leave this control empty, IQL places all documents into a single group. [Read more][aggregating]. | 
| **select** | Specifies the metrics to compute for each aggregated group. If you leave this control empty, IQL returns a count of all documents in each group. [Read more][metrics]. |
|  **Settings > Row limit** | Specifies the maximum number of rows to return. When you run any query, all rows are computed and cached, even if you specify a row limit. This means that specifying a row limit has no effect on the load the query places on the backend. |

###Additional options

- **TSV** and **CSV** allow you to download the query results as a tab-separated or comma-separated file.
- **Bash script** downloads a file for you to use to download query results from the command line.
- **Syntax guide** links to this documentation.
- **Pivot** allows you to summarize your data and change views. [Read more][pivot]. 
- **Graph** allows you to present your data in a graph format.

###Sharing a Query
To share a query with others, copy the URL line.

###Running Multiple Queries at the Same Time
To create a second query, click **+** to the right of the query. This action copies the contents of all cells in the new, second query. To clone a cell from the top row to the rows that follow, click the blue down arrow.

[timerange]: {{ site.baseurl }}/docs/timerange
[limiting]: {{ site.baseurl }}/docs/limiting
[aggregating]: {{ site.baseurl }}/docs/aggregating
[metrics]: {{ site.baseurl }}/docs/metrics
[pivot]: {{ site.baseurl }}/docs/pivot