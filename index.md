---
layout: default
title: Imhotep
exclude_toc: true
---

Imhotep is a large-scale analytics platform built by Indeed. For more details, check out the following tech talks:  

[Interactive Analytics with Imhotep](http://youtu.be/LBDZFtqL-ck?list=UURVEh0SlyrZNTeIbEDwj3wQ) gives an overview of what Imhotep is and how you can use it to get the most out of your data.

[Machine Learning at Indeed: Scaling Decision Trees](http://engineering.indeed.com/talks/machine-learning-indeed-scaling-decision-trees/) explains how we developed Imhotep, a distributed system for building decision trees for machine learning.

[Imhotep: Large-Scale Analytics and Machine Learning at Indeed](http://engineering.indeed.com/talks/imhotep-large-scale-analytics-machine-learning-indeed/) describes Imhotep’s primitive operations that allow us to build decision trees, drill into data, build graphs, and even execute SQL-like queries in IQL (Imhotep Query Language). The talk also covered what makes Imhotep fast, highly available, and fault tolerant.

[Large-Scale Interactive Analytics with Imhotep](http://engineering.indeed.com/talks/large-scale-interactive-analytics-with-imhotep/) shows how our engineering and product organizations use Imhotep to focus on key metrics at scale. 

## Features
Imhotep is a highly scalable analytics platform that lets you do the following:

- Perform fast, interactive, ad hoc queries and aggregate results for large datasets 
- Combine results from multiple time-series datasets
- Build your own data tools for analysis, monitoring, reporting, and automated data processing on top of the Imhotep platform

At Indeed, we use Imhotep to answer the following and many more questions about how people around the world are using our job search engine:

- How many unique job queries were performed on a specific day in a specific country?
- What are the top 50 queries in a specific country? How many times did job seekers click on a search result for each of those queries?
- Which job titles have the highest click-through rate for the query `Architecture` in the US? Which titles have the lowest click-through rate?


## Getting Started
See the [quick start page]({{ site.baseurl }}/docs/quick-start) for instructions. 

## Discussion
Ask and answer questions in our Q&A forum for Imhotep: [indeedeng-imhotep-users](https://groups.google.com/forum/#!forum/indeedeng-imhotep-users)

## See Also
[Apache Hadoop with Pig](http://pig.apache.org/)<br>
[Druid](http://druid.io/)<br>
[OpenDremel](https://code.google.com/p/dremel/)

## License

[Apache License Version 2.0](https://github.com/indeedeng/imhotep/blob/master/LICENSE) 
