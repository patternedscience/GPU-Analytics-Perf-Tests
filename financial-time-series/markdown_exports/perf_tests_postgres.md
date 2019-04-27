
## Postgres perf tests
### Querying the financial time-series (per-minute ETFs) data from a table


```python
import sys
import os
import csv
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime
import pytz
import time
```


```python
try:
    conn = psycopg2.connect(dbname='tests', user='hitchhiker', host='localhost', password='freeride', port='9478')
except:
    print('I am unable to connect to the database')
    
cur = conn.cursor()
```

### Descriptive statistics


```python
%%timeit -n1 -r1
cur.execute("SELECT  avg(volume), variance(volume) FROM public.symbols_minute;")
print(cur.fetchall()[0])
```

    (11881.6996761872, 10852793799.246)
    3.05 s ± 0 ns per loop (mean ± std. dev. of 1 run, 1 loop each)


#### Repeating the query to observe the performance improvement with the out-of-the-box caching feature (if any):


```python
%%timeit -n1 -r3
cur.execute("SELECT  avg(volume), variance(volume) FROM public.symbols_minute;")
print(cur.fetchall()[0])
```

    (11881.6996761872, 10852793799.233)
    (11881.6996761872, 10852793799.2329)
    (11881.6996761872, 10852793799.2329)
    3.06 s ± 5.53 ms per loop (mean ± std. dev. of 3 runs, 1 loop each)


### Sorting


```python
%%timeit -n1 -r1
sqlQuery='''
SELECT symbol_id, datetime, volume
FROM public.symbols_minute
ORDER BY volume DESC
LIMIT 1;
'''
cur.execute(sqlQuery)
print(cur.fetchall()[0])
```

    (41, datetime.datetime(2008, 11, 21, 16, 0), 116022000.0)
    11.4 s ± 0 ns per loop (mean ± std. dev. of 1 run, 1 loop each)


#### Repeating the query to observe the performance improvement with the out-of-the-box caching feature (if any):


```python
%%timeit -n1 -r3
sqlQuery='''
SELECT symbol_id, datetime, volume
FROM public.symbols_minute
ORDER BY volume DESC
LIMIT 1;
'''
cur.execute(sqlQuery)
print(cur.fetchall()[0])
```

    (41, datetime.datetime(2008, 11, 21, 16, 0), 116022000.0)
    (41, datetime.datetime(2008, 11, 21, 16, 0), 116022000.0)
    (41, datetime.datetime(2008, 11, 21, 16, 0), 116022000.0)
    11.4 s ± 7.98 ms per loop (mean ± std. dev. of 3 runs, 1 loop each)


### Mixed analytics (math ops + sorting):
#### Finding the top per-minute return


```python
%%timeit -n1 -r1
sqlQuery='''
SELECT *
FROM
(SELECT
symbol_id, datetime,
100*(close - open)/open AS return
FROM public.symbols_minute) t
ORDER BY return DESC
LIMIT 1
;
'''
cur.execute(sqlQuery)
print(cur.fetchall()[0])
```

    (46, datetime.datetime(2010, 5, 6, 17, 23), 22.5806540724312)
    13.2 s ± 0 ns per loop (mean ± std. dev. of 1 run, 1 loop each)


#### Repeating the query to observe the performance improvement with the out-of-the-box caching feature (if any):


```python
%%timeit -n1 -r3
sqlQuery='''
SELECT *
FROM
(SELECT
symbol_id, datetime,
100*(close - open)/open AS return
FROM public.symbols_minute) t
ORDER BY return DESC
LIMIT 1
;
'''
cur.execute(sqlQuery)
print(cur.fetchall()[0])
```

    (46, datetime.datetime(2010, 5, 6, 17, 23), 22.5806540724312)
    (46, datetime.datetime(2010, 5, 6, 17, 23), 22.5806540724312)
    (46, datetime.datetime(2010, 5, 6, 17, 23), 22.5806540724312)
    13.3 s ± 5.17 ms per loop (mean ± std. dev. of 3 runs, 1 loop each)


## License

Copyright (c) 2019, PatternedScience Inc.

This code was originally run on the [UniAnalytica](https://www.unianalytica.com) platform, is published by PatternedScience Inc. on [GitHub](https://github.com/patternedscience/GPU-Analytics-Perf-Tests) and is licensed under the terms of Apache License 2.0; a copy of the license is available in the GitHub repository.
