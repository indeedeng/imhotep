---
layout: default
title: Sample Queries
permalink: /docs/sample-queries/
---

What are the top 10 feeds that generated an Indeed apply for our organic results?  
With Imhotep it would take us several steps to get the result, by contrast by using IQL you can get the results much faster - http://go.indeed.com/IQLv7gr2e03gb

How many ZRP's were there in all countries and what percentage of searches where ZRP's?  
This particular question would take quite a bit of time to answer using Imhotep. However by using IQL we can create the necessary query to get all the information we want on one table. http://go.indeed.com/IQLsnjp4iic65

- our where is rcv:jsv since we want to filter out bot traffic
- group by is country since the question specifies we want to look at all countries
- select (which are our columns) is where it gets more advanced. We know we want 2 columns, 1 with total ZRP's and another with the ZRP rate. In order to do this we must use the function hasstr(). hasstr() takes a specific field available in that index. In this case we want to look at oji:0 so our hasstr function would be hasstr("oji:0"). We then multiply this value by count() to get the total number of ZRP's. In order to get the rate we must divide ZRP's by all other queries. This would look like this: hasstr("oji:0")*count()/count()

How many CTKs (cookies) did we receive by country (we need to use ctkrcvd for this).  
The only way it would be possible to do this in Imhotep would be to count up the number of lines for our query (because ctkrcvd is not an available metric). This however is not feasible since Imhotep only gives you a maximum of 10,000 lines and it would take a long time to check for every country. Here is what the IQL query would look like: http://go.indeed.com/IQLuem1nr10e7  
The key here is the use of the distinct() function in the select field. By using the distinct function it will tell us how many distinct values there are for a certain query. In this case we want the number of distinct CTKs received so we use distinct(ctkrcvd).


