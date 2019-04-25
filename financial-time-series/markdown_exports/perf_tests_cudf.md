
## cuDF perf tests
### Loading financial time-series (per-minute ETFs) data from CSV files into a cuDF and running the queries


```python
data_path = '/workspace/data/datasets/unianalytica/group/analytics-perf-tests/symbols/'
```


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

### 1.Load up all files to one cuDF DataFrame

#### Reading the CSV files into a Pandas DF (takes about 2 minutes - 63 files, 3.5 GB CSV format total size)


```python
symbol_dfs_list = []
records_count = 0
symbols_files = sorted(os.listdir(data_path))
for ix in range(len(symbols_files)):
    current_symbol_df = pd.read_csv(data_path + symbols_files[ix], parse_dates=[2], infer_datetime_format=True,
                                    names=['symbol_record_id', 'symbol', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'split_factor', 'earnings', 'dividends'])
    records_count = records_count + len(current_symbol_df)
    symbol_dfs_list.append(current_symbol_df)

print('Finished reading; now concatenating the DFs...')
symbols_pandas_df = pd.concat(symbol_dfs_list)
symbols_pandas_df.index = np.arange(records_count)
del(symbol_dfs_list)
print('Built a Pandas DF of {} records.'.format(records_count))
symbols_pandas_df.head()
```

    Finished reading; now concatenating the DFs...
    Built a Pandas DF of 50470570 records.





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



#### Building a cuDF from Pandas DF:
Replacing the `symbol` column here with `symbol_id`, as cuDF still cannot handle strings.


```python
symbols_list = sorted(pd.unique(symbols_pandas_df.symbol))
print(symbols_list)
```

    ['aaxj', 'acwi', 'agg', 'agq', 'bal', 'bik', 'biv', 'bkf', 'bnd', 'brf', 'bsv', 'bwx', 'csj', 'dag', 'dba', 'dbb', 'dbc', 'dbe', 'dbo', 'ddm', 'dem', 'dgaz', 'dgp', 'dgs', 'dia', 'dig', 'djp', 'dog', 'drn', 'drv', 'dto', 'dug', 'dust', 'dvy', 'dxd', 'dzz', 'ech', 'edc', 'edz', 'eeb', 'eem', 'eev', 'efa', 'emb', 'eny', 'epi', 'epp', 'epu', 'erx', 'ery', 'eum', 'euo', 'ewa', 'ewc', 'ewd', 'ewg', 'ewh', 'ewj', 'ewl', 'ewm', 'ewp', 'ews', 'ewt']



```python
keys = symbols_list
values = list(range(1, len(symbols_list)+1))
dictionary = dict(zip(keys, values))
symbols_pandas_df.insert(0, 'symbol_id', np.array([dictionary[x] for x in symbols_pandas_df.symbol.values]))
symbols_pandas_df_cudf = symbols_pandas_df.drop('symbol', axis=1)
symbols_pandas_df_cudf.head()
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




```python
symbols_pandas_df_cudf.dtypes
```




    symbol_id                    int64
    symbol_record_id             int64
    datetime            datetime64[ns]
    open                       float64
    high                       float64
    low                        float64
    close                      float64
    volume                     float64
    split_factor               float64
    earnings                   float64
    dividends                  float64
    dtype: object



### Now, building the cuDF from Pandas DF:


```python
symbols_gdf = cudf.DataFrame.from_pandas(symbols_pandas_df_cudf)
del(symbols_pandas_df_cudf)
print(symbols_gdf)
```

       symbol_id symbol_record_id                datetime  open  high   low close ... dividends
     0         1                0 2008-08-15T12:44:00.000 43.07 43.07 43.07 43.07 ...       0.0
     1         1                1 2008-08-15T16:00:00.000 43.07 43.07 43.07 43.07 ...       0.0
     2         1                2 2008-08-18T09:28:00.000 42.63 42.75 42.63 42.75 ...       0.0
     3         1                3 2008-08-18T09:30:00.000 42.77 42.77 42.77 42.77 ...       0.0
     4         1                4 2008-08-18T10:07:00.000 42.53 42.53 42.53 42.53 ...       0.0
     5         1                5 2008-08-18T10:43:00.000  42.4  42.4  42.4  42.4 ...       0.0
     6         1                6 2008-08-18T10:53:00.000  42.4  42.4  42.4  42.4 ...       0.0
     7         1                7 2008-08-18T12:04:00.000 42.24 42.24 42.23 42.23 ...       0.0
     8         1                8 2008-08-18T12:44:00.000  42.1  42.1  42.1  42.1 ...       0.0
     9         1                9 2008-08-18T16:00:00.000  42.1  42.1  42.1  42.1 ...       0.0
    [50470560 more rows]
    [3 more columns]


### 2.Perf Tests

#### 2.1 Descriptive statistics


```python
%%timeit -n1 -r3
print('Trading volume stats: mean of {}, variance of {}'.format(symbols_gdf['volume'].mean(), symbols_gdf['volume'].var()))
```

    Trading volume stats: mean of 11881.699675937818, variance of 10852793799.4029
    Trading volume stats: mean of 11881.699675937818, variance of 10852793799.4029
    Trading volume stats: mean of 11881.699675937818, variance of 10852793799.4029
    8.6 ms ± 185 µs per loop (mean ± std. dev. of 3 runs, 1 loop each)


#### 2.2 Sorting


```python
%%timeit -n1 -r3
print(symbols_gdf[['symbol_id', 'datetime', 'volume']].sort_values(by='volume', ascending=False).head(1))
```

      symbol_id                datetime      volume
    28629282        41 2008-11-21T16:00:00.000 116022000.0
      symbol_id                datetime      volume
    28629282        41 2008-11-21T16:00:00.000 116022000.0
      symbol_id                datetime      volume
    28629282        41 2008-11-21T16:00:00.000 116022000.0
    375 ms ± 2.83 ms per loop (mean ± std. dev. of 3 runs, 1 loop each)


#### 2.3 Mixed analytics (math ops + sorting) [finding the top per-minute return]:


```python
%%timeit -n1 -r3
symbols_gdf['return'] = 100*(symbols_gdf['close']-symbols_gdf['open'])/symbols_gdf['open']
print(symbols_gdf[['symbol_id', 'datetime', 'return']].sort_values(by='return', ascending=False).head(1))
```

      symbol_id                datetime             return
    33060823        46 2010-05-06T17:23:00.000 22.580645161290334
      symbol_id                datetime             return
    33060823        46 2010-05-06T17:23:00.000 22.580645161290334
      symbol_id                datetime             return
    33060823        46 2010-05-06T17:23:00.000 22.580645161290334
    314 ms ± 4.05 ms per loop (mean ± std. dev. of 3 runs, 1 loop each)


## License

Copyright (c) 2019, PatternedScience Inc.

This code was originally run on the [UniAnalytica](https://www.unianalytica.com) platform, is published by PatternedScience Inc. on [GitHub](https://github.com/patternedscience/GPU-Analytics-Perf-Tests) and is licensed under the terms of Apache License 2.0; a copy of the license is available in the GitHub repository.
