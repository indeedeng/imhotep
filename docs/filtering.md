---
layout: default
title: Filtering the Query
permalink: /docs/filtering/
---

Use the optional **where** filter to limit the query to only those documents that match the criteria you specify. Available fields are specific to the dataset you selected in **from**. 

Use these rules when constructing the **where** statement:

- Join separate filters with the default and optional AND operator or a space. For example, the following two **where** statements are identical: <br><br>
  * <code>country=us and language=en</code>
  * <code>country=us language=en</code>
- You cannot join separate filters with the OR operator.
- To negate a filter, precede the definition with <code>-</code> (minus sign).
- If you leave this control empty, IQL considers all documents. 

The following filters are available:
<table>
  <tr>
  <th>Filter</th>
    <th>Syntax</th>
    <th>Examples</th>
  </tr>
  <tr>
    <td>Field/term pairs</td>
    <td>field=term<br>field="term"<br>field:term<br>field!=term<br>field:""</td>
    <td><code>country=greatbritain<br>country="great britain"<br>country:japan<br>country!=us <br>location:""</code> returns the queries with an empty value for the string field <code>location</code>.</td>
  </tr>
  <tr>
    <td>Regular expressions<br>IQL uses <a href="http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html">Java 7 Pattern syntax</a></td>
    <td>field=~regex</td>
    <td><code>query=~".<strong>online marketing.</strong>"</code> returns the top queries that contain the substring <code>online marketing</code>.</td>
  </tr>
  <tr>
    <td>Metric/integer pairs</td>
   <td>metric=integer<br>metric!=integer<br>metric&#92;&#60;integer<br>metric&#60;=integer<br>metric&#62;integer<br>metric&#62;=integer</td>
    <td><code>clicks+impressions>5</code></td>
  </tr>
  <tr>
    <td>IN construction for including more than one term</td>
    <td>field in (term,term)<br>field in ("term",term) <br>field not in (term,term)</td>
    <td><code>country in (greatbritain,france)</code><br><code>country in ("great britain",france)</code><br><code>country not in (canada,us,germany)</code></td>
  </tr>
  <tr>
  	<td>To construct the following two filters, you must use the lucene() function:
    <ul><li>a logical OR of conditions on different fields</li>
        <li>filter by a range of strings like <code>field:[a TO b]</code></li></ul></td>
        <td>lucene("luceneQueryStr")</td>
        <td><code>lucene("(-resultA:0) OR (-resultB:0)")</code> returns the number of documents in the dataset that result in at least one <code>resultA</code> or one <code>resultB</code>.</td>
    
   <tr>
    <td>The sample() function allows you to retain a portion of the documents. The sampling denominator is 100 if you don't specify a value. <br><br>By default, rerunning the query retrieves a different set of documents. Provide a consistent value for the randomSeed parameter to retrieve the same documents when you rerun the query.</td>
    <td>sample(field, samplingRatioNumerator, [samplingRatioDenominator=100])<br><br>sample(field, samplingRatioNumerator, [samplingRatioDenominator=100], [randomSeed])</td>
    <td><code>sample(accountid, 1)</code> returns 1% of account IDs.<br> <code>sample(accountid, 1, 1000)</code> returns .1% of account IDs.</td>
  </tr>
</table>


