---
layout: default
title: Filtering the Query
permalink: /docs/filtering/
---

Use the optional **where** filter to limit the query to only those documents that match the criteria you specify. Available fields are specific to the index you selected in **from**. 

Use these rules when constructing the **where** statement:

- Separate filters by a space or `and`. The **where** filter does not support `or`.
- To negate a filter, precede the definition with `-` (minus sign).
- If you leave this control empty, IQL considers all documents. 

The following filters are available:
<table>
  <tr>
  <th>Filter</th>
    <th>Syntax</th>
    <th>Examples</th>
  </tr>
  <tr>
    <td valign="top">Field/term pairs</td>
    <td valign="top">field=term<br>field="term"<br>field:term<br>field!=term</td>
    <td valign="top"> `country=greatbritain`<br>`country="great britain"`<br>`country:japan`<br>`country!=us` </td>
  </tr>
  <tr>
    <td valign="top">Regular expressions<br>IQL uses [Java 7 Pattern syntax](http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html)</td>
    <td valign="top">field=~regex</td>
    <td valign="top">`query=~".*online marketing.*"` returns the top queries that contain the substring `online marketing`. </td>
  </tr>
  <tr>
    <td valign="top">Metric/integer pairs</td>
   <td valign="top">metric=integer<br>metric!=integer<br>metric<integer<br>metric<=integer<br>metric>integer<br>metric>=integer</td>
    <td valign="top">`clicks+impressions>5`</td>
  </tr>
  <tr>
    <td valign="top">IN construction for including more than one term.</td>
    <td valign="top">field in (term,term)<br>field in ("term",term) <br>field not in (term,term)</td>
    <td valign="top">`country in (greatbritain,france)`<br>`country in ("great britain",france)`<br>`country not in (canada,us,germany)`</td>
  </tr>
  <tr>
  	<td valign="top">To construct the following two filters, you must use the lucene() function:
    <ul><li>a logical OR of conditions on different fields</li>
        <li>filter by a range of strings like `field:[a TO b]`</li></ul></td>
        <td valign="top">lucene("luceneQueryStr")</td>
        <td valign="top">`lucene("(-resultA:0) OR (-resultB:0)")` returns the number of documents in the index that result in at least one `resultA` or one `resultB`.</td>
    </td>    
   <tr>
    <td valign="top">The sample() function allows you to retain a portion of the documents. The sampling denominator is 100 if you don't specify a value. <br><br>By default, rerunning the query retrieves a different set of documents. Provide a consistent value for the randomSeed parameter to retrieve the same documents when you rerun the query.</td>
    <td valign="top">sample(field, samplingRatioNumerator, [samplingRatioDenominator=100])<br><br>sample(field, samplingRatioNumerator, [samplingRatioDenominator=100], [randomSeed])</td>
    <td valign="top">`sample(accountid, 1)` returns 1% of account IDs. `sample(accountid, 1, 1000)` returns .1% of account IDs.</td>
  </tr>
</table>


