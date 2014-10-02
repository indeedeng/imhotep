---
layout: default
title: Overview
permalink: /docs/overview/
---

Data requests use IQL, a query language based on the SQL standard. You’ll use IQLWeb to specify data requests. The web app is a single page interface with a query editor at the top of the page. Each IQL query produces a results table below the editor.

To launch IQLWeb, open a browser and navigate to the IQL URL provided from the AWS configuration. The query editor includes the following controls:
<table>
  <tr>
    <th>Control</th>
    <th>Description</th>
  </tr>
  <tr>
    <td valign="top">`from`</td>
    <td valign="top">The name of the index you created in Imhotep TSV Uploader. You cannot edit the list of indexes in IQLWeb. </td>
  <tr>
    <td valign="top">`timerange`</td>
    <td valign="top">The required time range filter. [Read more][timerange]. </td>
  <tr>
    <td valign="top">`where`</td>
    <td valign="top">Specifies which documents to include in the query. Fields that are available are appropriate to the index you selected in from. If you leave this control empty, IQL considers all documents. [Read more][limiting].  </td>
  <tr>
    <td valign="top">`group by`</td>
    <td valign="top">Specifies how to group the documents so that you can retrieve aggregated stats. If you leave this control empty, IQLWeb places all documents into a single group. [Read more][aggregating]. </td>
  <tr>
    <td valign="top">`select`</td>
    <td valign="top">Specifies the metrics to compute for each aggregated group. If you leave this control empty, IQLWeb returns a count of all documents in each group. [Read more][metrics].</td>
  <tr>
    <td valign="top">`Settings > Row limit`</td>
    <td valign="top">Specifies the maximum number of rows to return. When you run any query, all rows are computed and cached, even if you specify a row limit. This means that specifying a row limit has no effect on the load the query places on the backend.</td>
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
To share a query with others, copy the URL line.

###Running Multiple Queries Simultaneously
IQLWeb can implicitly join multiple result tables from multiple IQL queries. This feature allows you to see data from multiple indexes and time ranges or filtered in different ways at one time.

By default, IQLWeb shows a single row for a single IQL query. To specify an additional query in a new row, click **+** to the right of the query. This action copies the contents of all cells from the original query in the new query. 

To clone a cell from the top row to the rows that follow, click the blue down arrow. Cloning a cell is useful if you want to keep some cell values in multiple queries synchronized: change the value of the top cell and click the arrow, instead of manually editing each query.

NOTE: Multiple queries should produce a result set that is consistent (has the same columns) and can be joined meaningfully. To run an independent query, open a new IQLWeb window. To quickly clone a page, select Alt+D, Alt+Enter.

###Keyboard Shortcuts
| | |
| ---- | ----- |
| Enter | Executes the current query set when one of the query elements is in focus. |
| Ctrl+Enter | Executes the current query set regardless of where the focus is. |
| Left / Right / Ctrl+Left / Ctrl+Right  | Moves the cursor inside and between the query part text boxes as if it was a single long text box. |
| Ctrl+Up / Ctrl+Down | Moves the cursor vertically between queries while staying in the same query part column. |
| Ctrl+C / Ctrl+V  | With no text selected, allows you to copy or paste the complete query from/to all the text boxes in that query line. |
| Ctrl+Shift+V | Allows you to paste a value that looks like a query (for example, starts with 'from' or 'select') but shouldn't be treated as one.  |
| Escape | If open, hides the autocomplete list. |
| Ctrl+Space | If closed, opens the autocomplete list. |
| Alt+D, Alt+Enter | Clones the current page in a new browser tab. |



[timerange]: {{ site.baseurl }}/docs/timerange
[limiting]: {{ site.baseurl }}/docs/limiting
[aggregating]: {{ site.baseurl }}/docs/aggregating
[metrics]: {{ site.baseurl }}/docs/metrics
[pivot]: {{ site.baseurl }}/docs/pivot