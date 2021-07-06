---
layout: default
title: Filtering the Query
permalink: /docs/filtering/
---

{::options parse_block_html="true" /}

Use the optional <strong>where</strong> filter to limit the query to only those documents that match the criteria you specify. Available fields are specific to the dataset you selected in <strong>from</strong>. 

Use these rules when constructing the <strong>where</strong> statement:



<ul>
  <li>Join separate filters with the default and optional AND operator or a space. For example, the following two <strong>where</strong> statements are identical: <br><br>
    <ul>
      <li><code>country=us and language=en</code></li>
      <li><code>country=us language=en</code></li>
    </ul>
  </li>
  <li>You cannot join separate filters with the OR operator.</li>
  <li>To negate a filter, precede the definition with <code>-</code> (minus sign).</li>
  <li>If you leave this control empty, IQL considers all documents.</li>
</ul>


<p>The following filters are available:</p>

<div class="table-wrapper">

|Filter|Syntax|Examples|
|------|------|--------|
|Field/term pairs|field=term<br>field="term"<br>field:term<br>field!=term<br>field:""| country=greatbritain<br>country="great britain"<br>country:japan<br>country!=us <br>location:""<code>returns the queries with an empty value for the string field</code><br><code>'location.</code>|
|Regular expressions<br>IQL uses <a href="http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html">Java 7 Pattern syntax</a>|field=~regex| <code>query=~".<strong>online marketing.</strong>"</code> returns the top queries that contain the substring <code>online marketing</code>.|
|Metric/integer pairs|metric=integer<br>metric!=integer<br>metric\&lt;integer<br>metric&lt;=integer<br>metric&gt;integer<br>metric&gt;=integer|<code>clicks+impressions&gt;5</code>|
|IN construction for including more than one term|field in (term,term)<br>field in ("term",term) <br>field not in (term,term)|<code>country in (greatbritain,france)</code><br><code>country in ("great britain",france)</code><br><code>country not in (canada,us,germany)</code>|
|To construct the following two filters, you must use the lucene() function: <br> A logical OR of conditions on different fields. <br> Filter by a range of strings like <code>field:[a TO b]</code>|lucene("luceneQueryStr")|<code>lucene("(-resultA:0) OR (-resultB:0)")</code> returns the number of documents in the dataset that result in at least one <code>resultA</code> or one <code>resultB</code>.|
|The sample() function allows you to retain a portion of the documents. The sampling denominator is 100 if you don't specify a value. <br>By default, rerunning the query retrieves a different set of documents. Provide a consistent value for the randomSeed parameter to retrieve the same documents when you rerun the query.|sample(field, samplingRatioNumerator, [samplingRatioDenominator=100])<br>sample(field, samplingRatioNumerator, [samplingRatioDenominator=100], [randomSeed])|<code>sample(accountid, 1)</code> returns 1% of account IDs.<br> <code>sample(accountid, 1, 1000)</code> returns .1% of account IDs.|

</div>

