---
layout: default
title: Computing Metrics
permalink: /docs/metrics/
---

Use the optional **select** clause to compute metrics for your aggregated groups. Use these rules when constructing the **select** statement:

- Separate metric statements or distinct() function calls by commas.
- If you leave this control empty, IQL returns a count of all documents in each group. 

The following metrics are available:
<table>
  <tr>
    <th>Metric</th>
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
       <li>floatscale(...) converts floating point numbers stored in strings to scaled integers.</li>
     </ul>
       
</td>
    <td valign="top">count()<br>exp(metric,scalingFactor)<br>floatscale(field,scale,offset)</td>
    <td valign="top">`count()` <br>`floatscale(float,10,5)` multiplies each value in `float` by 10 and then adds 5 to each product.</td>
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
    <td valign="top">Filters: using a filter as a metric returns a count of all of the documents that match the filter. Adding `/count()` returns the average. <br><br>[Read more about filters and filter syntax](../filtering/).</td>
    <td valign="top">field="term"<br>field=integer<br>field=~regex<br>metric!=integer<br>metric\<integer<br>metric<=integer<br>metric>integer<br>metric>=integer<br>field in (term,term)<br>field not in (term,term)<br>lucene("luceneQueryStr")</td>
    <td valign="top">`country="us"` returns the number of documents with a value of `us` for `country`.  You must include `""` around a string term.<br><br>`clicks=1` returns the number of documents with a value of `1` for `clicks`.<br><br>`revenue>500`<br>`group="mobile"/count()`</td>
  </tr>

</table>

