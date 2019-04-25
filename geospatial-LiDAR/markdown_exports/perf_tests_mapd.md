
## OmniSci/MapD Core DB perf tests
### Querying 1 tile of [aerial LiDAR scan of Montreal](http://donnees.ville.montreal.qc.ca/dataset/lidar-aerien-2015) from a table


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




```python
print(list(con.execute("SELECT count(*) FROM montreal_lidar_1tile;"))[0][0])
```

    18306827


### Cropping a building out of the neighborhood point cloud and counting the points


```python
%%timeit -n1 -r1
sqlQuery='''
SELECT count(*)
FROM montreal_lidar_1tile
WHERE ST_CONTAINS( ST_GeomFromText('POLYGON((-73.567650 45.518485, -73.567179 45.518275, -73.566918 45.518577, -73.567297 45.518780, -73.567650 45.518485))', 4326) , P);
'''
print(list(con.execute(sqlQuery))[0][0])
```

    77475
    268 ms ± 0 ns per loop (mean ± std. dev. of 1 run, 1 loop each)


#### Repeating the query to observe the performance improvement with the out-of-the-box caching feature:


```python
%%timeit -n1 -r3
sqlQuery='''
SELECT count(*)
FROM montreal_lidar_1tile
WHERE ST_CONTAINS( ST_GeomFromText('POLYGON((-73.567650 45.518485, -73.567179 45.518275, -73.566918 45.518577, -73.567297 45.518780, -73.567650 45.518485))', 4326) , P);
'''
print(list(con.execute(sqlQuery))[0][0])
```

    77475
    77475
    77475
    31 ms ± 8.52 ms per loop (mean ± std. dev. of 3 runs, 1 loop each)


## License

Copyright (c) 2019, PatternedScience Inc.

This code was originally run on the [UniAnalytica](https://www.unianalytica.com) platform, is published by PatternedScience Inc. on [GitHub](https://github.com/patternedscience/GPU-Analytics-Perf-Tests) and is licensed under the terms of Apache License 2.0; a copy of the license is available in the GitHub repository.
