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
    <td valign="top">metric</td>
    <td valign="top">`revenue`</td>
  </tr>
  <tr>
    <td valign="top">Arithmetic expression of two metrics (+, -, *, %) executed on each document. <br>The divide operation (/) is executed in aggregate after everything else is computed. If you require a per document divide, use a reverse slash (\\). </td>
    <td valign="top">metric+metric<br>metric\-metric<br>metric\*metric<br>metric%metric<br>metric/metric</td>
    <td valign="top">`clicks/impressions`<br>`revenue-expenses`</td>
  </tr>
  <tr>
    <td valign="top">Function calls:
    <ul>
       <li>count() returns the number of documents in the group. Each document has an implicit value of 1.</li>
       <li>exp(...) applies the Math.exp() function to the specified metric. The scalingFactor defaults to 1.  </li>
       <li>field="term" and field=integer return the number of documents with the defined field value.</li>
       <li>floatscale(...) converts floating point numbers stored in strings to scaled integers.</li>
     </ul>
       
</td>
    <td valign="top">count()<br>exp(metric,scalingFactor)<br>field="term"<br>field=integer<br>floatscale(field,scale,offset)</td>
    <td valign="top">`count()` <br>`country="us"` returns the number of documents with a value of `us` for `country`. <br>`clicks=1` returns the number of documents with a value of `1` for `clicks`.<br>`floatscale(float,10,5)` multiplies each value in `float` by 10 and then adds 5 to each product.</td>
  </tr>
  <tr>
    <td valign="top">distinct()</td>
    <td valign="top">distinct(field)</td>
    <td valign="top">`distinct(country)` returns a count of distinct terms for the `country` field in each grouping.</td>
  </tr>
  <tr>
    <td valign="top">percentile()</td>
    <td valign="top">percentile(field,&nbsp;N)</td>
    <td valign="top">`percentile(totaltime, 50)` returns the median value of `totaltime`.</td>
  </tr>
    <tr>
    <td valign="top">Inequality</td>
    <td valign="top">metric!=integer<br>metric\<integer<br>metric<=integer<br>metric>integer<br>metric>=integer</td>
    <td valign="top">`revenue>500`</td>
  </tr>

</table>

