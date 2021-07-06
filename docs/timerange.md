---
layout: default
title: Filtering on a Time Range
permalink: /docs/timerange/
---

The required **timerange** filter allows you to specify a time range with a calendar popup or by entering the start and end dates:

<div class="table-wrapper">

<table>
  <tr>
    <th>Method</th>
    <th>Syntax</th>
    <th>Examples</th>
  </tr>
  <tr>
    <td>Enter the start and end dates.</td>
    <td>ISO date format: YYYY-MM-DD </td>
    <td><code>2014-01-01</code></td>
  </tr>
  <tr>
    <td>Enter start and end dates that include time.</td>
    <td>YYYY-MM-DD HH:MM:SS<br>YYYY-MM-DDTHH:MM:SS<br>“YYYY-MM-DD HH:MM:SS”</td>
    <td><code>2014-01-01 12:00:00</code><br><code>2014-01-01T12:00:00</code><br><code>"2014-01-01 12:00:00"</code></td>
  </tr>
  <tr>
    <td>Enter a relative value.</td>
    <td>Natural language terms: <br>yesterday <br>today <br>tomorrow<br><br>Terms for some time period ago: <br>Nyears <br>Nmonths<br>Nweeks <br>Ndays <br>Nhours <br>Nminutes <br>Nseconds</td>
    <td><code>yesterday</code><br><code>5d</code><br><code>5days</code><br><code>5daysago</code><br><code>"5 days ago"</code></td>
  </tr>
</table>
</div>

