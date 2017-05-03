---
layout: default
title: Aggregating Results
permalink: /docs/aggregating/
---

Use the optional **group by** clause to group documents and retrieve aggregated stats. Separate each group definition from another with a comma. Unlike SQL, if you leave this control empty, the IQL web client places all documents into a single group and returns one row.

The following group definitions are available:
<table>
  <tr>
    <th>Group Definition</th>
    <th>Syntax</th>
    <th>Examples</th>
  </tr>
  <tr>
    <td valign="top">Simple grouping by field name</td>
    <td valign="top">field</td>
    <td valign="top"><code>country</code></td>
  </tr>
  <tr>
    <td valign="top">Limit the number of groups (top/bottom K).</td>
    <td valign="top">field[number] <br> field[bottom number by metric]</td>
    <td valign="top"><code>country[5]</code> returns the top 5 countries by count.<br><code>country[bottom 5 by clicks]</code> specifies the metric by which to order and uses the bottom countries instead of the top.</td>
  </tr>
<tr>
    <td valign="top">Group your data into buckets by ranges you define. The values for min, min2, max, max2, interval and interval2 are numbers.<br><br>Multiple bucket statements are allowed. <br><br>If you include all bucket definitions in one statement, the size of the buckets is automatically determined. <br><br>Group your data into time buckets. The bucket size uses the same syntax as the relative values for the start and end values in the <strong>timerange</strong> filter. For example: Nd or Ndays. [Read more about relative values][timerange].<br><br>You can also specify the number of buckets as an absolute value.<br><br>The time() call cannot be defined inside buckets().
 </td>
    <td valign="top">buckets(metric, min, max, interval)<br><br>buckets(metricX, min, max, interval metricY, min2, max2, interval2)<br><br>time(bucketSize)<br><br>time(Nb)</td>
    <td valign="top"><code>buckets(accountbalance, 0, 100, 20)</code><br><br><code>time(1h)</code> groups data into buckets, each of which includes data from 1 hour.<br><br><code>time(3b)</code> groups data into 3 buckets, each of which includes data from one-third of the given time range.</td>
  </tr>
<tr>
    <td valign="top">IN construction for including more than one term. Using the IN construction in the <strong>group by</strong> clause is the same as using the IN construction in the <strong>where</strong> filter and then grouping by field name.</td>
    <td valign="top">field in (term,term)<br>field in ("term",term) <br>field not in (term,term) </td>
    <td valign="top"><code>country in (canada,us)</code> <br><code>country in ("great britain",deutschland)</code> <br><code>country not in (france,canada)</code> </td>
  </tr>

</table>


[timerange]: {{ site.baseurl }}/docs/timerange