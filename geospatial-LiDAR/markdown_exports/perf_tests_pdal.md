
## PDAL perf tests
### Filtering/Cropping a building out of 1 tile of [aerial LiDAR scan of Montreal](http://donnees.ville.montreal.qc.ca/dataset/lidar-aerien-2015)


```python
import pdal
print(pdal.__version__)
```

    2.1.8


### Stats of the whole tile:


```python
!pdal info /workspace/data/tmp/299-5042_2015.laz
```

    {
      "filename": "\/workspace\/data\/tmp\/299-5042_2015.laz",
      "pdal_version": "1.8.0 (git-version: b62d4f)",
      "stats":
      {
        "bbox":
        {
          "EPSG:4326":
          {
            "bbox":
            {
              "maxx": -73.56143962,
              "maxy": 45.5269008,
              "maxz": 153.13,
              "minx": -73.57425137,
              "miny": 45.51789481,
              "minz": -32.38
            },
            "boundary": {
    	"coordinates" : 
    	[
    		[
    			[
    				-73.574239539999994,
    				45.517894810000001
    			],
    			[
    				-73.574251369999999,
    				45.526893200000004
    			],
    			[
    				-73.561449420000002,
    				45.5269008
    			],
    			[
    				-73.561439620000002,
    				45.517902409999998
    			],
    			[
    				-73.574239539999994,
    				45.517894810000001
    			]
    		]
    	],
    	"type" : "Polygon"
    }
    
          },
          "native":
          {
            "bbox":
            {
              "maxx": 300000,
              "maxy": 5043000,
              "maxz": 153.13,
              "minx": 299000,
              "miny": 5042000,
              "minz": -32.38
            },
            "boundary": {
    	"coordinates" : 
    	[
    		[
    			[
    				299000.0,
    				5042000.0
    			],
    			[
    				299000.0,
    				5043000.0
    			],
    			[
    				300000.0,
    				5043000.0
    			],
    			[
    				300000.0,
    				5042000.0
    			],
    			[
    				299000.0,
    				5042000.0
    			]
    		]
    	],
    	"type" : "Polygon"
    }
    
          }
        },
        "statistic":
        [
          {
            "average": 299511.6762,
            "count": 18306827,
            "maximum": 300000,
            "minimum": 299000,
            "name": "X",
            "position": 0,
            "stddev": 293.9903706,
            "variance": 86430.33801
          },
          {
            "average": 5042486.228,
            "count": 18306827,
            "maximum": 5043000,
            "minimum": 5042000,
            "name": "Y",
            "position": 1,
            "stddev": 1214.935307,
            "variance": 1476067.8
          },
          {
            "average": 44.38390359,
            "count": 18306827,
            "maximum": 153.13,
            "minimum": -32.38,
            "name": "Z",
            "position": 2,
            "stddev": 10.93573669,
            "variance": 119.5903369
          },
          {
            "average": 27102.1784,
            "count": 18306827,
            "maximum": 65535,
            "minimum": 655,
            "name": "Intensity",
            "position": 3,
            "stddev": 15846.0598,
            "variance": 251097611.2
          },
          {
            "average": 1.157659271,
            "count": 18306827,
            "maximum": 6,
            "minimum": 1,
            "name": "ReturnNumber",
            "position": 4,
            "stddev": 0.4212197869,
            "variance": 0.1774261088
          },
          {
            "average": 1.315239064,
            "count": 18306827,
            "maximum": 6,
            "minimum": 1,
            "name": "NumberOfReturns",
            "position": 5,
            "stddev": 0.5913093299,
            "variance": 0.3496467237
          },
          {
            "average": 0,
            "count": 18306827,
            "maximum": 0,
            "minimum": 0,
            "name": "ScanDirectionFlag",
            "position": 6,
            "stddev": 0,
            "variance": 0
          },
          {
            "average": 0,
            "count": 18306827,
            "maximum": 0,
            "minimum": 0,
            "name": "EdgeOfFlightLine",
            "position": 7,
            "stddev": 0,
            "variance": 0
          },
          {
            "average": 4.193251294,
            "count": 18306827,
            "maximum": 28,
            "minimum": 1,
            "name": "Classification",
            "position": 8,
            "stddev": 4.964387529,
            "variance": 24.64514353
          },
          {
            "average": -2.592689274,
            "count": 18306827,
            "maximum": 23,
            "minimum": -24,
            "name": "ScanAngleRank",
            "position": 9,
            "stddev": 11.40947765,
            "variance": 130.1761802
          },
          {
            "average": 205,
            "count": 18306827,
            "maximum": 205,
            "minimum": 205,
            "name": "UserData",
            "position": 10,
            "stddev": 0.04791233473,
            "variance": 0.002295591819
          },
          {
            "average": 102.5502547,
            "count": 18306827,
            "maximum": 265,
            "minimum": 19,
            "name": "PointSourceId",
            "position": 11,
            "stddev": 109.1514185,
            "variance": 11914.03217
          },
          {
            "average": 304763.3929,
            "count": 18306827,
            "maximum": 537947.1777,
            "minimum": 270975.5952,
            "name": "GpsTime",
            "position": 12,
            "stddev": 81211.82358,
            "variance": 6595360289
          }
        ]
      }
    }


### PDAL pipeline to crop the building:


```python
pipeline_json = """
{
  "pipeline":[
    "/workspace/data/tmp/299-5042_2015.laz",
    {
      "type":"filters.crop",
      "a_srs":"EPSG:4326",
      "polygon":"POLYGON((-73.567650 45.518485, -73.567179 45.518275, -73.566918 45.518577, -73.567297 45.518780, -73.567650 45.518485))"

    },
    {
      "type":"writers.las",
      "filename":"/workspace/data/tmp/299-5042_2015_EPSG4326_building_cropped.las"
    }
  ]
}"""
```

### Running the above pipeline


```python
%%timeit -n1 -r3

pipeline = pdal.Pipeline(pipeline_json)
pipeline.validate()
pipeline.loglevel = 8
count = pipeline.execute()
```

    17 s ± 118 ms per loop (mean ± std. dev. of 3 runs, 1 loop each)


### Stats of the cut building:


```python
!pdal info /workspace/data/tmp/299-5042_2015_EPSG4326_building_cropped.las
```

    {
      "filename": "\/workspace\/data\/tmp\/299-5042_2015_EPSG4326_building_cropped.las",
      "pdal_version": "1.8.0 (git-version: b62d4f)",
      "stats":
      {
        "bbox":
        {
          "EPSG:4326":
          {
            "bbox":
            {
              "maxx": -73.56691831,
              "maxy": 45.51877839,
              "maxz": 153.13,
              "minx": -73.56764979,
              "miny": 45.51827728,
              "minz": 40.11
            },
            "boundary": {
    	"coordinates" : 
    	[
    		[
    			[
    				-73.567649189999997,
    				45.51827728
    			],
    			[
    				-73.567649790000004,
    				45.518777960000001
    			],
    			[
    				-73.566918900000005,
    				45.518778390000001
    			],
    			[
    				-73.566918310000005,
    				45.51827771
    			],
    			[
    				-73.567649189999997,
    				45.51827728
    			]
    		]
    	],
    	"type" : "Polygon"
    }
    
          },
          "native":
          {
            "bbox":
            {
              "maxx": 299572.01,
              "maxy": 5042097.69,
              "maxz": 153.13,
              "minx": 299514.91,
              "miny": 5042042.05,
              "minz": 40.11
            },
            "boundary": {
    	"coordinates" : 
    	[
    		[
    			[
    				299514.90999999997,
    				5042042.0499999998
    			],
    			[
    				299514.90999999997,
    				5042097.6900000004
    			],
    			[
    				299572.01000000001,
    				5042097.6900000004
    			],
    			[
    				299572.01000000001,
    				5042042.0499999998
    			],
    			[
    				299514.90999999997,
    				5042042.0499999998
    			]
    		]
    	],
    	"type" : "Polygon"
    }
    
          }
        },
        "statistic":
        [
          {
            "average": 299547.6313,
            "count": 77408,
            "maximum": 299572.01,
            "minimum": 299514.91,
            "name": "X",
            "position": 0,
            "stddev": 1076.705621,
            "variance": 1159294.993
          },
          {
            "average": 5042067.919,
            "count": 77408,
            "maximum": 5042097.69,
            "minimum": 5042042.05,
            "name": "Y",
            "position": 1,
            "stddev": 18122.61041,
            "variance": 328429008.2
          },
          {
            "average": 93.90220662,
            "count": 77408,
            "maximum": 153.13,
            "minimum": 40.11,
            "name": "Z",
            "position": 2,
            "stddev": 29.38741988,
            "variance": 863.6204474
          },
          {
            "average": 27358.2821,
            "count": 77408,
            "maximum": 65535,
            "minimum": 655,
            "name": "Intensity",
            "position": 3,
            "stddev": 15422.61731,
            "variance": 237857124.8
          },
          {
            "average": 1.079759198,
            "count": 77408,
            "maximum": 4,
            "minimum": 1,
            "name": "ReturnNumber",
            "position": 4,
            "stddev": 0.2816322366,
            "variance": 0.07931671668
          },
          {
            "average": 1.161598284,
            "count": 77408,
            "maximum": 4,
            "minimum": 1,
            "name": "NumberOfReturns",
            "position": 5,
            "stddev": 0.3914778699,
            "variance": 0.1532549226
          },
          {
            "average": 0,
            "count": 77408,
            "maximum": 0,
            "minimum": 0,
            "name": "ScanDirectionFlag",
            "position": 6,
            "stddev": 0,
            "variance": 0
          },
          {
            "average": 0,
            "count": 77408,
            "maximum": 0,
            "minimum": 0,
            "name": "EdgeOfFlightLine",
            "position": 7,
            "stddev": 0,
            "variance": 0
          },
          {
            "average": 5.929335469,
            "count": 77408,
            "maximum": 28,
            "minimum": 1,
            "name": "Classification",
            "position": 8,
            "stddev": 3.146797678,
            "variance": 9.902335624
          },
          {
            "average": 4.592483981,
            "count": 77408,
            "maximum": 20,
            "minimum": -20,
            "name": "ScanAngleRank",
            "position": 9,
            "stddev": 15.06506012,
            "variance": 226.9560363
          },
          {
            "average": 205,
            "count": 77408,
            "maximum": 205,
            "minimum": 205,
            "name": "UserData",
            "position": 10,
            "stddev": 0.7368239654,
            "variance": 0.542909556
          },
          {
            "average": 28.5915926,
            "count": 77408,
            "maximum": 31,
            "minimum": 25,
            "name": "PointSourceId",
            "position": 11,
            "stddev": 2.646359798,
            "variance": 7.003220181
          },
          {
            "average": 0,
            "count": 77408,
            "maximum": 0,
            "minimum": 0,
            "name": "Red",
            "position": 12,
            "stddev": 0,
            "variance": 0
          },
          {
            "average": 0,
            "count": 77408,
            "maximum": 0,
            "minimum": 0,
            "name": "Green",
            "position": 13,
            "stddev": 0,
            "variance": 0
          },
          {
            "average": 0,
            "count": 77408,
            "maximum": 0,
            "minimum": 0,
            "name": "Blue",
            "position": 14,
            "stddev": 0,
            "variance": 0
          },
          {
            "average": 274049.4163,
            "count": 77408,
            "maximum": 275247.8654,
            "minimum": 273271.2692,
            "name": "GpsTime",
            "position": 15,
            "stddev": 1323.565709,
            "variance": 1751826.187
          }
        ]
      }
    }


## License

Copyright (c) 2019, PatternedScience Inc.

This code was originally run on the [UniAnalytica](https://www.unianalytica.com) platform, is published by PatternedScience Inc. on [GitHub](https://github.com/patternedscience/GPU-Analytics-Perf-Tests) and is licensed under the terms of Apache License 2.0; a copy of the license is available in the GitHub repository.
