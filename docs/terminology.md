---
layout: default
title: Terminology
permalink: /docs/terminology/
---

## aggregate function
The process of adding summary data to a dataset. 

## document
A collection of data related to one entity, such as a person or company. A document is similar to a database record or a row in a database table.

## field
A component of your dataset that contains a single attribute of the dataset. A field is similar to a column in a database table. In IQL, you reference fields in **where**, **group by** and **select**.

## dataset
Your data files that TSV Uploader has converted so that Imhotep can access the data. In IQL, you select the dataset in **from**.

## metric
A numeric field in your document. 

## shard
Data that has been partitioned horizontally (based on time) such that datasets can reside on multiple servers. Distributing the dataset across multiple machines allows processing times to be faster.
