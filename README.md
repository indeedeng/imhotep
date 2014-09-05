# Imhotep
Imhotep is [a data analytics platform](http://engineering.indeed.com/talks/large-scale-interactive-analytics-with-imhotep/) we built at Indeed to do large scale analytics. 

# Documentation
http://indeed.github.io/imhotep

# Modifying documentation
- All documentation pages' permalink must end with a "/"
    - Without a trailing slash, the content will be served with content-type "application/octect-stream" and will be downloaded instead of displayed in your web browser
    - http://pixelcog.com/blog/2013/jekyll-from-scratch-core-architecture/#pitfalls_with_pretty_urls
- When building a link, use `{{ site.baseurl }}` as the href prefix
    - GOOD: `{{ site.baseurl }}/docs/new/page/`
    - BAD: `/docs/new/page/` - This will work locally but will not work when deployed to `http://indeedeng.github.io/proctor`
- GFM ([github-flavored-markdown](https://help.github.com/articles/github-flavored-markdown)) is NOT available in the markdown for the documentation.

# Discussion

Join the [indeedeng-proctor-users](https://groups.google.com/d/forum/indeedeng-proctor-users) mailing list to ask questions and discuss use of Proctor. QUESTION: is there an Imhotep forum?

# References
- http://jekyllrb.com/
- https://help.github.com/articles/using-jekyll-with-pages
- https://help.github.com/articles/pages-don-t-build-unable-to-run-jekyll
- https://github.com/vmg/redcarpet
