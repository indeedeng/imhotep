---
layout: default
title: Filtering on a Time Range
permalink: /docs/timerange/
---

The required **timerange** filter allows you to specify a time range with a calendar popup or by entering the start and end dates:
<table>
  <tr>
    <th>Method</th>
    <th>Format</th>
    <th>Examples</th>
  </tr>
  <tr>
    <td valign="top">Enter the start and end dates.</td>
    <td valign="top">ISO date format: YYYY-MM-DD </td>
    <td valign="top">`2014-01-01` </td>
  </tr>
  <tr>
    <td valign="top">Enter start and end dates that include time.</td>
    <td valign="top">YYYY-MM-DD HH:MM:SS<br>YYYY-MM-DDTHH:MM:SS<br>“YYYY-MM-DD HH:MM:SS”</td>
    <td valign="top">`2014-01-01 12:00:00`<br>`2014-01-01T12:00:00`<br>`"2014-01-01 12:00:00"`</td>
  </tr>
  <tr>
    <td valign="top">Enter a relative value.</td>
    <td valign="top">Natural language terms: <br>yesterday <br>today <br>tomorrow<br><br>Terms for some time period ago: <br>Nyears <br>Nmonths<br>Nweeks <br>Ndays <br>Nhours <br>Nminutes <br>Nseconds</td>
    <td valign="top">`yesterday`<br>`5d`<br>`5days`<br>`5daysago`<br>`"5 days ago"`</td>
  </tr>
</table>

