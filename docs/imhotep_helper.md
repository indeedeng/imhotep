---
layout: default
title: Imhotep Upload Format Helper Script
permalink: /docs/imhotep_helper/
---

The Python script [imhotep_helper.py](/imhotep/files/imhotep_helper.py) is a combination linter/converter that will make sure that your TSV or CSV data is formatted properly for upload to Imhotep.

* Summarizes the type inference (integer vs. string) for your columns
* Handles time conversion so that your times are adjusted to Imhotep's default time zone
* Cleans up data in a number of ways to conform with TSV or CSV formats
* Rewrites data into a file named properly for the time range of the data it contains, since the file name timestamp ranges determine the Imhotep shards  and must correspond properly to the contained data.

For more information, see [Data File Requirements](../data-file-requirements).

## Usage

```
usage: imhotep_helper.py [-h] [-l] [-c] [-n NONINT] [-i INDEX]
                         [--prefix PREFIX] [-f FORMAT] [-o OFFSET]
                         [datafile]

positional arguments:
  datafile              filename of data to upload

optional arguments:
  -h, --help            show this help message and exit
  -l, --lint            check file for problems. if specified, overrides
                        --convert.
  -c, --convert         automatically fix problems and convert
  -n NONINT, --nonint NONINT
                        name or index of column to display non-integer values
                        (name must be a valid index name)
  -i INDEX, --index INDEX
                        index of timestamp field
  --prefix PREFIX       prefix of converted filename. default = "converted_"
  -f FORMAT, --format FORMAT
                        format of timestamp field. defaults include
                        "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y", "%m/%d/%Y
                        %H:%M:%S", "%a %b %d %H:%M:%S %Y",
                        "%Y-%m-%dT%H:%M:%S". (see
                        https://docs.python.org/2/library/datetime.html
                        #strftime-strptime-behavior for details)
  -o OFFSET, --offset OFFSET
                        GMT offset of timestamps. default is -6.
```

## Example Output (lint mode)

```
Using GMT offset -6
detected tsv file type

WARN [78751]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/shuttle/missions/sts-69/\www.pic.net"
WARN [182501]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/shuttle/missions/sts-71/images/http:\\www.mca.com"
WARN [336170]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/history/\"
WARN [336177]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/history/\"
WARN [336336]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/history/\"
WARN [535266]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/\\espnet.sportszone.com"
WARN [615192]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/HISTORY/APOLLO/HTTP:\\POPULARMECHANICS.COM"
WARN [615206]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/HISTORY/APOLLO/HTTP:\\POPULARMECHANICS.COM"
WARN [873938]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/\\www.isisnet.com/home/newnetscape.html"
WARN [968770]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/history\apollo"
WARN [968787]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/history\apollo"
WARN [1123563]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/shuttle/missions/sts-71/images/http:\\www.yahoo.com"
WARN [1228814]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/shuttle/technology\images"
WARN [1364399]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/history/apollo/apollo.html\"
WARN [1364538]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/history/apollo/apollo.html\"
WARN [1449354]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/facilities/www.commerce.com/\\www.commerce.com\cmt"
WARN [1520084]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/shuttle/missions/\\www.cinday-cga.com:p09\"
WARN [1520124]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/shuttle/missions/\\www.cinday-cga.com:p09\"
WARN [1749804]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/software\"
WARN [1836424]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/\\www.yahoo.com"
WARN [1836560]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/shuttle/missions/mission.html\"
WARN [1836561]: Found an escape character (\) in entry. TSVs do not support quoting. Entry: "/shuttle/missions/mission.html\"

SUMMARY STATS:

Total records: 1891714

Field "host":
- 0.0% int values
- 100.0% string values
> Will be treated as a string field.

Field "time":
> Will be converted to unixtime

Field "method":
- 0.0% int values
- 100.0% string values
> Will be treated as a string field.

Field "url":
- 0.0% int values
- 100.0% string values
> Will be treated as a string field.

Field "version":
- 0.2% int values
- 99.8% string values
> Will be treated as a string field.

Field "response":
- 99.0% int values
- 0.0% string values
> Will be treated as an int field. Non-int values will be discarded.

Field "bytes":
- 98.8% int values
- 0.0% string values
> Will be treated as an int field. Non-int values will be discarded.

```