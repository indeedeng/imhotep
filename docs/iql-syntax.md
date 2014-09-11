---
layout: default
title: IQL Syntax
permalink: /docs/iql-syntax/
---

why do I care about IQL? It helps to answer more complex questions:

- What are the 5 countries that created the most X?
- Get the unique number of values for a given category?

how to navigate to IQL  
UI field definitions  

|  |  |
| --------- | ------------- |
| **from** | Refers to the index (or dataset) you created in TSV Uploader. The list is prepopulated. |
| **timerange** | The time range from which to query your index. | 
| **where** | Fields appropriate to the index you selected in from appear when you click in the where box. Here you enter the parameters that you want to look at.  [Read more](#where)  |
| **group by** | How you want your results grouped.   [Read more](#groupby) | 
| **select** | the metric you want as a column. Fields defined with the int datatype. [Read more](#select) |

Other options:

- TSV and CSV allow you to download the query result as a tab-separated or comma-separated file.
- Bash script downloads a file for you to use to download query results from the command line.
- Syntax guide links to this documentation.
- Settings allow you to define a row limit for your query results.

Add multiple queries: click +. Queries can be from different indices.

### timerange
Select dates from a calendar or enter dates and time in this format: YYYY-MM-DD 00:00:00. You can also enter a relative value like *5d* (5 days) or *yesterday* and *today*.

### where


### <a name="groupby"></a>group by
think of this as the rows you would like in your results, you use group by to specify how you want your results grouped. For example, you could decide to group things by country. You can also group your query by multiple parameters by separating each group with a comma (example, grouping by qnorm and lnorm). Note: When you do multiple group by's the system will first group by your first field and then group by the second field. So using our qnorm, lnorm example the system would first look up the top qnorms and then figure out the to lnorms for those qnorms.

### select
To add more than one metric write a comma in between each one. You can also do simple math with the metrics (+ - * /), however note that the system will not allow you to do more than one computation (ex. you cannot write (ojc/oji)*100, but you can write ojc/oji

