---
layout: default
title: Overview
permalink: /docs/overview/
---

To explore your Imhotep data, you can use IQL, a query language similar to SQL. You can use the IQL web client to send queries to your Imhotep cluster. The client is a single page interface with a query editor at the top of the page. Each IQL query produces a result table below the editor.

To launch the IQL web client, open a browser and navigate to the IQL URL provided when you created the cluster on AWS. The query editor includes the following controls:
<table>
   <tr>
    <td valign="top">`from`</td>
    <td valign="top">The name of the index you created in Imhotep TSV Uploader. You cannot edit the list of indexes in the client. </td>
  <tr>
    <td valign="top">`timerange`</td>
    <td valign="top">The required time range filter. [Read more][timerange]. </td>
  <tr>
    <td valign="top">`where`</td>
    <td valign="top">A conditional expression that specifies which documents to include in the results. Available fields are specific to the index you selected in **from**. If you leave this control empty, IQL considers all documents in the time range given. [Read more][filtering].  </td>
  <tr>
    <td valign="top">`group by`</td>
    <td valign="top">A list of expressions that specify how to group the documents for aggregated stats. If you leave this control empty, the client places all documents into a single group. [Read more][aggregating]. </td>
  <tr>
    <td valign="top">`select`</td>
    <td valign="top">A comma-separated list of the metrics to compute for each aggregated group. If you leave this control empty (equivalent to entering the `count()` expression), the client returns a count of all documents in each group. [Read more][metrics].</td>
  <tr>
    <td valign="top">`Settings > Row limit`</td>
    <td valign="top">The maximum number of rows to return. When you run any query, all rows are computed and cached, even if you specify a row limit. This means that specifying a row limit has no effect on the load the query places on the backend.</td>
</table>


###Additional options

| | |
| ---- | ----- |
| `TSV` and `CSV` allow you to download the query results as a tab-separated or comma-separated file. |
| `Bash script` downloads a file for you to use to download query results from the command line. |
| `Syntax guide` links to this documentation. |
| `Pivot` allows you to summarize your query results and change views. [Read more][pivot]. |
| `Graph` allows you to present your query results in a graph format. |

###Sharing a Query
To share a query with others, copy the URL.

###Running Multiple Queries Simultaneously
The IQL web client can implicitly join multiple result tables from multiple IQL queries. This feature allows you to see data from multiple indexes and time ranges or filtered in different ways at one time.

By default, the client shows a single row for a single IQL query. To specify an additional query in a new row, click **+** to the right of the query. This action copies the contents of all cells from the original query in the new query. 

To clone a cell from the top row to the rows that follow, click the blue down arrow. Cloning a cell is useful if you want to keep some cell values in multiple queries synchronized: change the value of the top cell and click the arrow, instead of manually editing each query.

NOTE: Multiple queries should produce a result set that is consistent (has the same columns) and can be joined meaningfully. To run an independent query, open a new client window. To quickly clone a page, press Alt+D, Alt+Enter. [Click for more shortcuts][shortcuts].




[timerange]: {{ site.baseurl }}/docs/timerange
[filtering]: {{ site.baseurl }}/docs/filtering
[aggregating]: {{ site.baseurl }}/docs/aggregating
[metrics]: {{ site.baseurl }}/docs/metrics
[pivot]: {{ site.baseurl }}/docs/pivot
[shortcuts]: {{ site.baseurl }}/docs/shortcuts