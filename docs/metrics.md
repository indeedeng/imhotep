---
layout: default
title: Computing Metrics
permalink: /docs/metrics/
---

Use the optional **select** filter to compute metrics for your aggregated groups. Use these rules when constructing the **select** statement:

- Separate metric statements or distinct() function calls by commas.
- If you leave this control empty, IQL returns a count of all documents in each group. 

The following filters are available:
<table>
  <tr>
    <th>Filter</th>
    <th>Syntax</th>
    <th>Examples</th>
  </tr>
  <tr>
    <td valign="top">Simple metric name</td>
    <td valign="top">DESCRIBE indexName</td>
    <td valign="top"></td>
  </tr>
  <tr>
    <td valign="top">Arithmetic expression of two metrics (+, -, *, %) executed on each document. The divide operation (/) is executed in aggregate after everything else is computed. If you require a per document divide, use a reverse slash (/). </td>
    <td valign="top"></td>
    <td valign="top"></td>
  </tr>
  <tr>
    <td valign="top">Function calls</td>
    <td valign="top">count()<br>cached(metric...)<br>exp(...)<br>dynamic(metric…)<br>hasstr(field, term)<br>hasstr(“field:term”)<br>hasint(field, term)<br>hasint(“field:term”)<br></td>
    <td valign="top"></td>
  </tr>
  <tr>
    <td valign="top">distinct()</td>
    <td valign="top">distinct(field)</td>
    <td valign="top">`distinct(country)` returns a count of distinct terms for the country field in each grouping.</td>
  </tr>
  <tr>
    <td valign="top">percentile()</td>
    <td valign="top">percentile(field, N)</td>
    <td valign="top">`percentile(totaltime, 50)` returns the median value of `totaltime`.</td>
  </tr>
</table>

