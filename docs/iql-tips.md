---
layout: default
title: Aggregating Results
permalink: /docs/iql-tips/
---
## Table of Contents

* [Grouping](#grouping)
* [Query Smaller Time Ranges](#query-smaller-time-ranges)


<sub>Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc.go)</sub>

## Grouping

### **Don't un-invert IQL indices by grouping all fields**
Imhotep stores IQL indices inverted, which makes it difficult to see the original documents as rows. In other tools with tabular data, you can un-invert an index by grouping by every available field. Indeed does NOT recommended this methods when working with IQL indices, as it's very inefficient. If you need access to complete rows of data, consider alternative data sources such as MySQL, HBase and LogRepo.

### **Get the number of groups to be returned**
If you think a grouping may result in a massive number of groups, you can first get the actual number of expected groups. Run a **DISTINCT** query: 
"from jobsearch 1h today select distinct(ctkrcvd)" returns the number of groups you would get by running "from jobsearch 1h today group by ctkrcvd"

### **Put the largest grouping as the last grouping**
If you put the largest grouping as the last grouping, the result of the last largest grouping is streamed instead of stored in memory. For example, since there are dozens of countries but millions of unique queries, prefer "group by country, q" to "group by q, country" and "group by q[500000], country[50]".

## Query Smaller Time Ranges

### **Test on a small time range**
First, test your query on a small time range. Then, you can ramp up to the required range as you gauge that performance is sufficient. For example, "from jobsearch 1h today ..." is a better way to test than "from jobsearch 180d today ...".

###