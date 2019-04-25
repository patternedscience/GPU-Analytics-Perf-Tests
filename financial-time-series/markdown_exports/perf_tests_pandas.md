
## Pandas perf tests
### Loading financial time-series (per-minute ETFs) data from CSV files into a Pandas DF and running the queries


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



### 2.Perf Tests

#### 2.1 Descriptive statistics


```python
%%timeit -n1 -r3
print('Trading volume stats: mean of {}, variance of {}'.format(symbols_df['volume'].mean(), symbols_df['volume'].var()))
```

    Trading volume stats: mean of 11881.69967593782, variance of 10852793799.402876
    Trading volume stats: mean of 11881.69967593782, variance of 10852793799.402876
    Trading volume stats: mean of 11881.69967593782, variance of 10852793799.402876
    673 ms ± 1.13 ms per loop (mean ± std. dev. of 3 runs, 1 loop each)


#### 2.2 Sorting


```python
%%timeit -n1 -r3
symbols_df_sorted = symbols_df[['symbol_id', 'datetime', 'volume']].sort_values(by='volume', ascending=False)
print(symbols_df_sorted.iloc[0].values)
# Should not sort the DF inplace to avoid skewing the results for subsequent runs
```

    [41 Timestamp('2008-11-21 16:00:00') 116022000.0]
    [41 Timestamp('2008-11-21 16:00:00') 116022000.0]
    [41 Timestamp('2008-11-21 16:00:00') 116022000.0]
    15.1 s ± 130 ms per loop (mean ± std. dev. of 3 runs, 1 loop each)


#### 2.3 Mixed analytics (math ops + sorting) [finding the top per-minute return]:


```python
%%timeit -n1 -r3
symbols_df['return'] = 100*(symbols_df['close']-symbols_df['open'])/symbols_df['open']
print(symbols_df[['symbol_id', 'datetime', 'return']].sort_values(by='return', ascending=False).head(1))
```

              symbol_id            datetime     return
    33060823         46 2010-05-06 17:23:00  22.580645
              symbol_id            datetime     return
    33060823         46 2010-05-06 17:23:00  22.580645
              symbol_id            datetime     return
    33060823         46 2010-05-06 17:23:00  22.580645
    10.7 s ± 1.33 s per loop (mean ± std. dev. of 3 runs, 1 loop each)


## License

Copyright (c) 2019, PatternedScience Inc.

This code was originally run on the [UniAnalytica](https://www.unianalytica.com) platform, is published by PatternedScience Inc. on [GitHub](https://github.com/patternedscience/GPU-Analytics-Perf-Tests) and is licensed under the terms of Apache License 2.0; a copy of the license is available in the GitHub repository.
