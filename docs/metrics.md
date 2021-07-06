---
layout: default
title: Computing Metrics
permalink: /docs/metrics/
---

Use the optional **select** clause to compute metrics for your aggregated groups. Use these rules when constructing the **select** statement:

- Separate metric statements or distinct() function calls by commas.
- If you leave this control empty, IQL returns a count of all documents in each group. 

The following metrics are available:

<div class="table-wrapper">

<table>
  <tr>
    <th>Metric</th>
    <th>Syntax</th>
    <th>Examples</th>
  </tr>
  <tr>
    <td valign="top">Simple metric name</td>
    <td valign="top">metric</td>
    <td valign="top"><code>revenue</code></td>
  </tr>
  <tr>
    <td>Arithmetic expression of two metrics (+, -, *, %) executed on each document. <br>The divide operation (/) is executed in aggregate after everything else is computed. If you require a per document divide, use a reverse slash (\\). </td>
    <td>metric+metric<br>metric\-metric<br>metric\*metric<br>metric%metric<br>metric/metric</td>
    <td><code>clicks/impressions</code><br><code>revenue-expenses</code></td>
  </tr>
  <tr>
    <td>Function calls:
    <ul>
       <li>count() returns the number of documents in the group. Each document has an implicit value of 1.</li>
       <li>exp(...) applies the Math.exp() function to the specified metric. The scalingFactor defaults to 1.  </li>
       <li>floatscale(...) converts floating point numbers stored in strings to scaled integers.</li>
     </ul>
       
</td>
    <td>count()<br>exp(metric,scalingFactor)<br>floatscale(field,scale,offset)</td>
    <td><code>count()</code> <br><code>floatscale(float,10,5)</code> multiplies each value in <code>float</code> by 10 and then adds 5 to each product.</td>
  </tr>
  <tr>
    <td>distinct()</td>
    <td>distinct(field)</td>
    <td><code>distinct(country)</code> returns a count of distinct terms for the <code>country</code> field in each grouping.</td>
  </tr>
  <tr>
    <td>percentile()</td>
    <td>percentile(field,&nbsp;N)</td>
    <td><code>percentile(totaltime, 50)</code> returns the median value of <code>totaltime</code>.</td>
  </tr>
    <tr>
    <td>Return a count of all of the documents that match the expression. Adding <code>/count()</code> returns the average. You must include <code>""</code> around a string term. </td>
    <td>field="term"<br>field=integer<br>metric!=integer<br>metric\<integer<br>metric<=integer<br>metric>integer<br>metric>=integer<br>lucene("luceneQueryStr")</td>
    <td><code>country="us"</code> returns the number of documents with a value of <code>us</code> for <code>country</code>. <br><br><code>clicks=1</code> returns the number of documents with a value of <code>1</code> for <code>clicks</code>.<br><br><code>revenue>500</code><br><code>group="mobile"/count()</code></td>
  </tr>

</table>
</div>