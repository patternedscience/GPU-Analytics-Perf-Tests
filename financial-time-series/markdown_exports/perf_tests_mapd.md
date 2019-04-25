
## OmniSci (MapD) Core DB perf tests
### Querying the financial time-series (per-minute ETFs) data from a table


```python
import sys
import os
import csv
import pandas as pd
import numpy as np
import cudf
from pymapd import connect
import pyarrow as pa
import pandas as pd
from datetime import datetime
import pytz
import time
```


```python
uri = "mapd://mapd:HyperInteractive@localhost:6274/mapd?protocol=binary"
con = connect(uri=uri)
con
```




    Connection(mapd://mapd:***@localhost:6274/mapd?protocol=binary)



### Descriptive statistics


```python
%%timeit -n1 -r1
print(list(con.execute("SELECT  avg(volume), variance(volume) FROM symbols_minute;"))[0])
```

    (11881.752968670653, 10852793799.409428)
    459 ms ± 0 ns per loop (mean ± std. dev. of 1 run, 1 loop each)


#### Repeating the query to observe the performance improvement with the out-of-the-box caching feature:


```python
%%timeit -n1 -r3
print(list(con.execute("SELECT  avg(volume), variance(volume) FROM symbols_minute;"))[0])
```

    (11881.752968670653, 10852793799.409428)
    (11881.752968670653, 10852793799.409428)
    (11881.752968670653, 10852793799.409428)
    49.5 ms ± 154 µs per loop (mean ± std. dev. of 3 runs, 1 loop each)


### Sorting


```python
%%timeit -n1 -r1
sqlQuery='''
SELECT symbol_id, record_timestamp, volume
FROM symbols_minute
ORDER BY volume DESC
LIMIT 1;
'''
print(list(con.execute(sqlQuery))[0])
```

    (41, datetime.datetime(2008, 11, 21, 16, 0), 116022000.0)
    401 ms ± 0 ns per loop (mean ± std. dev. of 1 run, 1 loop each)


#### Repeating the query to observe the performance improvement with the out-of-the-box caching feature:


```python
%%timeit -n1 -r3
sqlQuery='''
SELECT symbol_id, record_timestamp, volume
FROM symbols_minute
ORDER BY volume DESC
LIMIT 1;
'''
print(list(con.execute(sqlQuery))[0])
```

    (41, datetime.datetime(2008, 11, 21, 16, 0), 116022000.0)
    (41, datetime.datetime(2008, 11, 21, 16, 0), 116022000.0)
    (41, datetime.datetime(2008, 11, 21, 16, 0), 116022000.0)
    25 ms ± 494 µs per loop (mean ± std. dev. of 3 runs, 1 loop each)


### Mixed analytics (math ops + sorting):
#### Finding the top per-minute return


```python
%%timeit -n1 -r1
sqlQuery='''
SELECT *
FROM
(SELECT
symbol_id, record_timestamp,
100*(record_close - record_open)/record_open AS "return"
FROM symbols_minute) t
ORDER BY "return" DESC
LIMIT 1
;
'''
print(list(con.execute(sqlQuery))[0])
```

    (46, datetime.datetime(2010, 5, 6, 17, 23), 22.58065414428711)
    1.66 s ± 0 ns per loop (mean ± std. dev. of 1 run, 1 loop each)


#### Repeating the query to observe the performance improvement with the out-of-the-box caching feature:


```python
%%timeit -n1 -r3
sqlQuery='''
SELECT *
FROM
(SELECT
symbol_id, record_timestamp,
100*(record_close - record_open)/record_open AS "return"
FROM symbols_minute) t
ORDER BY "return" DESC
LIMIT 1
;
'''
print(list(con.execute(sqlQuery))[0])
```

    (46, datetime.datetime(2010, 5, 6, 17, 23), 22.58065414428711)
    (46, datetime.datetime(2010, 5, 6, 17, 23), 22.58065414428711)
    (46, datetime.datetime(2010, 5, 6, 17, 23), 22.58065414428711)
    49.6 ms ± 1.67 ms per loop (mean ± std. dev. of 3 runs, 1 loop each)


## License

Copyright (c) 2019, PatternedScience Inc.

This code was originally run on the [UniAnalytica](https://www.unianalytica.com) platform, is published by PatternedScience Inc. on [GitHub](https://github.com/patternedscience/GPU-Analytics-Perf-Tests) and is licensed under the terms of Apache License 2.0; a copy of the license is available in the GitHub repository.
