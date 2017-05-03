---
layout: default
title: Overview
permalink: /docs/overview/
---

To explore your Imhotep data, use IQL, which is a query language similar to SQL. You can use the IQL web client to send queries to your Imhotep cluster. The client is a single page interface with a query editor at the top of the page. Each IQL query produces a result table below the editor.

Logging into the IQL web client:

1. Open a browser and navigate to the IQL URL provided when you created the stack on AWS. 
2. Bypass the SSL warning to reach the login screen.
3. Enter your login ID and password that you defined during setup.
4. If you don't see a newly created dataset, clear your cache: select **Settings > Clear cache**.

The query editor includes the following controls:
<table>
   <tr>
    <td valign="top"><code>from</code></td>
    <td valign="top">The name of the dataset you created in Imhotep TSV Uploader. You cannot edit the list of datasets in the client. </td></tr>
  <tr>
    <td valign="top"><code>timerange</code></td>
    <td valign="top">The required time range filter. <a href="http://opensource.indeedeng.io/imhotep/docs/timerange/">Read more</a>. </td></tr>
  <tr>
    <td valign="top"><code>where</code></td>
    <td valign="top">A conditional expression that specifies which documents to include in the results. Available fields are specific to the dataset you selected in <strong>from</strong>. If you leave this control empty, IQL considers all documents in the time range given. <a href="http://opensource.indeedeng.io/imhotep/docs/filtering/">Read more</a>.  </td></tr>
  <tr>
    <td valign="top"><code>group by</code></td>
    <td valign="top">A list of expressions that specify how to group the documents for aggregated stats. If you leave this control empty, the client places all documents into a single group. <a href="http://opensource.indeedeng.io/imhotep/docs/aggregating/">Read more</a>. </td></tr>
  <tr>
    <td valign="top"><code>select</code></td>
    <td valign="top">A comma-separated list of the metrics to compute for each aggregated group. If you leave this control empty (equivalent to entering the <code>count()</code> expression), the client returns a count of all documents in each group. <a href="http://opensource.indeedeng.io/imhotep/docs/metrics/">Read more</a>.</td></tr>
  <tr>
    <td valign="top"><code>Settings > Row limit</code></td>
    <td valign="top">The maximum number of rows to return. When you run any query, all rows are computed and cached, even if you specify a row limit. This means that specifying a row limit has no effect on the load the query places on the backend.</td></tr>
</table>
<br>

As you construct your query, an autocomplete list shows available fields.

### Additional options

| | |
| ---- | ----- |
| `TSV` and `CSV` allow you to download the query results as a tab-separated or comma-separated file. |
| `Bash script` downloads a file for you to use to download query results from the command line. |
| `Help` links to this documentation. |
| `Pivot` allows you to summarize your query results and change views. [Read more][pivot]. |
| `Graph` allows you to present your query results in a graph format. |

### Sharing a Query
To share a query with others, copy the URL.

### Running Multiple Queries Simultaneously
The IQL web client can implicitly join multiple result tables from multiple IQL queries. This feature allows you to see data from multiple datasets and time ranges or filtered in different ways at one time. 

By default, the client shows a single row for a single IQL query. To specify an additional query in a new row, click **+** to the right of the query. This action copies the contents of all cells from the original query in the new query. 

To clone a cell from the top row to the rows that follow, click the blue down arrow. Cloning a cell is useful if you want to keep some cell values in multiple queries synchronized: change the value of the top cell and click the arrow, instead of manually editing each query.

NOTE: Multiple queries should produce a result set that is consistent (has the same columns) and can be joined meaningfully. To run an independent query, open a new client window. 






[timerange]: {{ site.baseurl }}/docs/timerange
[filtering]: {{ site.baseurl }}/docs/filtering
[aggregating]: {{ site.baseurl }}/docs/aggregating
[metrics]: {{ site.baseurl }}/docs/metrics
[pivot]: {{ site.baseurl }}/docs/pivot
[shortcuts]: {{ site.baseurl }}/docs/shortcuts