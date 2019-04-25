
## Script to load financial time-series (per-minute ETFs) data from CSV files into a Pandas DF and a Postgres table
### The ingestion for Pandas is also done in its own perf tests notebook


```python
data_path = '/workspace/data/datasets/unianalytica/group/analytics-perf-tests/symbols/'
```


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

### 1.Load up all files to one Pandas DF
Takes about 2 minutes (63 files, 3.5 GB CSV format total size)


```python
symbol_dfs_list = []
records_count = 0
symbols_files = sorted(os.listdir(data_path))
for ix in range(len(symbols_files)):
    current_symbol_df = pd.read_csv(data_path + symbols_files[ix], parse_dates=[2], infer_datetime_format=True,
                                    names=['symbol_record_id', 'symbol', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'split_factor', 'earnings', 'dividends'])
    records_count = records_count + len(current_symbol_df)
    symbol_dfs_list.append(current_symbol_df)
    #print('Loaded symbol #{}'.format(ix+1))

print('Now concatenating the DFs...')
symbols_df = pd.concat(symbol_dfs_list)
symbols_df.index = np.arange(records_count)
del(symbol_dfs_list)
```

    Now concatenating the DFs...


#### Adding `symbol_id` column


```python
symbols_list = sorted(pd.unique(symbols_df.symbol))
keys = symbols_list
values = list(range(1, len(symbols_list)+1))
dictionary = dict(zip(keys, values))
symbols_df.insert(0, 'symbol_id', np.array([dictionary[x] for x in symbols_df.symbol.values]))
symbols_df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>symbol_id</th>
      <th>symbol_record_id</th>
      <th>symbol</th>
      <th>datetime</th>
      <th>open</th>
      <th>high</th>
      <th>low</th>
      <th>close</th>
      <th>volume</th>
      <th>split_factor</th>
      <th>earnings</th>
      <th>dividends</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>0</td>
      <td>aaxj</td>
      <td>2008-08-15 12:44:00</td>
      <td>43.07</td>
      <td>43.07</td>
      <td>43.07</td>
      <td>43.07</td>
      <td>232.759</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>1</td>
      <td>aaxj</td>
      <td>2008-08-15 16:00:00</td>
      <td>43.07</td>
      <td>43.07</td>
      <td>43.07</td>
      <td>43.07</td>
      <td>116.379</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>2</td>
      <td>aaxj</td>
      <td>2008-08-18 09:28:00</td>
      <td>42.63</td>
      <td>42.75</td>
      <td>42.63</td>
      <td>42.75</td>
      <td>10143.600</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>3</td>
      <td>aaxj</td>
      <td>2008-08-18 09:30:00</td>
      <td>42.77</td>
      <td>42.77</td>
      <td>42.77</td>
      <td>42.77</td>
      <td>24439.700</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>4</td>
      <td>aaxj</td>
      <td>2008-08-18 10:07:00</td>
      <td>42.53</td>
      <td>42.53</td>
      <td>42.53</td>
      <td>42.53</td>
      <td>2327.590</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>
</div>



### 2.Import all files to a single table in the tests DB on Postgres


```python
symbols_files = sorted(os.listdir(data_path))
keys = symbols_files
values = list(range(1, len(symbols_files)+1))
dictionary = dict(zip(keys, values))
```


```python
try:
    conn = psycopg2.connect(dbname='tests', user='hitchhiker', host='localhost', password='freeride', port='9478')
except:
    print('I am unable to connect to the database')
    
cur = conn.cursor()
```


```python
sqlQuery = '''
DROP TABLE IF EXISTS public.symbols_minute;

DROP TABLE IF EXISTS public.symbols_minute_staging;
'''
cur.execute(sqlQuery)
conn.commit()
```


```python
sqlQuery = '''
CREATE TABLE public.symbols_minute
(
  symbol_id int,
  symbol_record_id int,
  symbol character varying(4),
  datetime timestamp without time zone NOT NULL,
  open real,
  high real,
  low real,
  close real,
  volume real,
  split_factor real,
  earnings real,
  dividends real
);

CREATE INDEX symbols_minute_datetime_idx ON public.symbols_minute (datetime);
'''
cur.execute(sqlQuery)
conn.commit()
    
sqlQuery = '''
CREATE TABLE public.symbols_minute_staging
(
  symbol_record_id int,
  symbol character varying(4),
  datetime timestamp without time zone PRIMARY KEY NOT NULL,
  open real,
  high real,
  low real,
  close real,
  volume real,
  split_factor real,
  earnings real,
  dividends real
);
'''
cur.execute(sqlQuery)
conn.commit()
```


```python
num_files = 0
for ix in range(len(symbols_files)):
    num_files += 1
    #if num_files > 2: break
    try:
        cur.execute("TRUNCATE TABLE public.symbols_minute_staging;")
        conn.commit()

        f = open(data_path + symbols_files[ix], 'r')
        cur.copy_from(f, 'symbols_minute_staging', sep=',')
        conn.commit()

        sqlQuery = '''
        SELECT COUNT(*)
        FROM public.symbols_minute_staging;
        '''
        cur.execute(sqlQuery)
        current_count = cur.fetchall()[0][0]
        
        sqlQuery = '''
        INSERT INTO symbols_minute
        SELECT '%(symbol_id)s', *
        FROM public.symbols_minute_staging;
        '''% {'symbol_id': dictionary[symbols_files[ix]]}
        cur.execute(sqlQuery)
        conn.commit()
        
        print('{} records from {} are imported'.format(current_count, symbols_files[ix]))
    except:
        print('Cound not import ' + symbols_files[ix])
        e = sys.exc_info()[0]
        print("Error: %s" % e)
            
print('Data import finished.')
```

    713747 records from aaxj.csv are imported
    740226 records from acwi.csv are imported
    1176236 records from agg.csv are imported
    744534 records from agq.csv are imported
    97408 records from bal.csv are imported
    209734 records from bik.csv are imported
    724294 records from biv.csv are imported
    311684 records from bkf.csv are imported
    971145 records from bnd.csv are imported
    329759 records from brf.csv are imported
    897540 records from bsv.csv are imported
    613534 records from bwx.csv are imported
    801695 records from csj.csv are imported
    151729 records from dag.csv are imported
    911223 records from dba.csv are imported
    396196 records from dbb.csv are imported
    1120318 records from dbc.csv are imported
    222477 records from dbe.csv are imported
    596675 records from dbo.csv are imported
    965754 records from ddm.csv are imported
    686974 records from dem.csv are imported
    738617 records from dgaz.csv are imported
    540873 records from dgp.csv are imported
    447393 records from dgs.csv are imported
    2205551 records from dia.csv are imported
    766858 records from dig.csv are imported
    706549 records from djp.csv are imported
    750583 records from dog.csv are imported
    550154 records from drn.csv are imported
    422018 records from drv.csv are imported
    465337 records from dto.csv are imported
    792790 records from dug.csv are imported
    876324 records from dust.csv are imported
    1339492 records from dvy.csv are imported
    1106553 records from dxd.csv are imported
    443106 records from dzz.csv are imported
    522919 records from ech.csv are imported
    880975 records from edc.csv are imported
    861640 records from edz.csv are imported
    397302 records from eeb.csv are imported
    1636423 records from eem.csv are imported
    578232 records from eev.csv are imported
    1586562 records from efa.csv are imported
    798735 records from emb.csv are imported
    111377 records from eny.csv are imported
    941161 records from epi.csv are imported
    1063864 records from epp.csv are imported
    308303 records from epu.csv are imported
    1015647 records from erx.csv are imported
    910479 records from ery.csv are imported
    379833 records from eum.csv are imported
    636708 records from euo.csv are imported
    1221628 records from ewa.csv are imported
    1221855 records from ewc.csv are imported
    612191 records from ewd.csv are imported
    1217676 records from ewg.csv are imported
    1306387 records from ewh.csv are imported
    1707118 records from ewj.csv are imported
    724760 records from ewl.csv are imported
    1087538 records from ewm.csv are imported
    757775 records from ewp.csv are imported
    1104913 records from ews.csv are imported
    1343489 records from ewt.csv are imported
    Data import finished.



```python
cur.execute("SELECT count(*) FROM public.symbols_minute;")
print(cur.fetchall()[0][0])
    
cur.execute("SELECT * FROM public.symbols_minute LIMIT 10;")
for row in cur.fetchall():
    print(row)
```

    50470570
    (1, 0, 'aaxj', datetime.datetime(2008, 8, 15, 12, 44), 43.07, 43.07, 43.07, 43.07, 232.759, 1.0, 0.0, 0.0)
    (1, 1, 'aaxj', datetime.datetime(2008, 8, 15, 16, 0), 43.07, 43.07, 43.07, 43.07, 116.379, 1.0, 0.0, 0.0)
    (1, 2, 'aaxj', datetime.datetime(2008, 8, 18, 9, 28), 42.63, 42.75, 42.63, 42.75, 10143.6, 1.0, 0.0, 0.0)
    (1, 3, 'aaxj', datetime.datetime(2008, 8, 18, 9, 30), 42.77, 42.77, 42.77, 42.77, 24439.7, 1.0, 0.0, 0.0)
    (1, 4, 'aaxj', datetime.datetime(2008, 8, 18, 10, 7), 42.53, 42.53, 42.53, 42.53, 2327.59, 1.0, 0.0, 0.0)
    (1, 5, 'aaxj', datetime.datetime(2008, 8, 18, 10, 43), 42.4, 42.4, 42.4, 42.4, 2327.59, 1.0, 0.0, 0.0)
    (1, 6, 'aaxj', datetime.datetime(2008, 8, 18, 10, 53), 42.4, 42.4, 42.4, 42.4, 2327.59, 1.0, 0.0, 0.0)
    (1, 7, 'aaxj', datetime.datetime(2008, 8, 18, 12, 4), 42.24, 42.24, 42.23, 42.23, 232.759, 1.0, 0.0, 0.0)
    (1, 8, 'aaxj', datetime.datetime(2008, 8, 18, 12, 44), 42.1, 42.1, 42.1, 42.1, 116.379, 1.0, 0.0, 0.0)
    (1, 9, 'aaxj', datetime.datetime(2008, 8, 18, 16, 0), 42.1, 42.1, 42.1, 42.1, 116.379, 1.0, 0.0, 0.0)


## License

Copyright (c) 2019, PatternedScience Inc.

This code was originally run on the [UniAnalytica](https://www.unianalytica.com) platform, is published by PatternedScience Inc. on [GitHub](https://github.com/patternedscience/GPU-Analytics-Perf-Tests) and is licensed under the terms of Apache License 2.0; a copy of the license is available in the GitHub repository.
