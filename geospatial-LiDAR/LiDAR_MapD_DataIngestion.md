# Steps to ingest LiDAR data into MapD Core DB

## Download file

```
mkdir /workspace/data/tmp && cd /workspace/data/tmp
wget http://depot.ville.montreal.qc.ca/geomatique/lidar_aerien/2015/299-5042_2015.laz
```

## Convert to CSV and reproject

```
docker run \
-v /workspace/data/tmp:/data:z \
pdal/pdal:1.8 pdal translate \
-i /data/299-5042_2015.laz -o /data/299-5042_2015.csv \
-f filters.reprojection \
--filters.reprojection.in_srs="EPSG:32188" \
--filters.reprojection.out_srs="EPSG:4326" \
--writers.text.order="X:7,Y:7,Z:2" \
--writers.text.quote_header="false"
```

### Notes

- "--filters.reprojection.in_srs="EPSG:32188"" is optional; it can be gotten out of the source Laz file;

- With writers.text.order, we mention the precision for each of X, Y and Z; the default precision is 3 which basically corrupts the LiDAR data;

- quote_header setting to false lets pdal read back the csv file as a source file;

- The above command uses the docker image, but it can be run almost as-is with other types of [PDAL](https://pdal.io/download.html) installations.

## Schema/Create MapD table and load data

```
CREATE TABLE montreal_lidar_1tile (
P GEOMETRY(POINT, 4326),
Z FLOAT,
Intensity INTEGER,
ReturnNumber TINYINT,
NumberOfReturns TINYINT,
ScanDirectionFlag TINYINT,
EdgeOfFlightLine TINYINT,
Classification TINYINT,
ScanAngleRank TINYINT,
UserData INTEGER,
PointSourceId TINYINT,
GpsTime FLOAT
);
```

```
mapdql> COPY montreal_lidar_1tile FROM '/workspace/data/tmp/299-5042_2015.csv' WITH (max_reject=100000);
Result
Loaded: 18306827 recs, Rejected: 0 recs in 62.520000 secs
```

The first 2 columns of the csv file are X and Y, but we can define a POINT field in the table and mapD merges the 2 automatically and creates the POINT field during import.

## License

Copyright (c) 2019, PatternedScience Inc.

This code was originally run on the [UniAnalytica](https://www.unianalytica.com) platform, is published by PatternedScience Inc. on [GitHub](https://github.com/patternedscience/GPU-Analytics-Perf-Tests) and is licensed under the terms of Apache License 2.0; a copy of the license is available in the GitHub repository.
