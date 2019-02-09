# Supercharging Analytics with GPUs

### A performance benchmark


### Introduction

This repo contains multiple performance tests and their results regarding how GPUs accelerate several analytics operations; we use these two types of datasets:

- Financial data (per-minute price for 63 ETFs)

- LiDAR data (aerial LiDAR scan of Montreal, Canada)

### Component versions evaluated

* [CUDA](https://developer.nvidia.com/cuda-toolkit) 10.0

* cuDF 0.5.0 (GPU DataFrame, part of NVIDIA's [RAPIDS AI](https://github.com/rapidsai))

* [OmniSci](https://www.omnisci.com/) (MapD Core DB) v4.4.2 (more specifically, [commit 568e77d](https://github.com/omnisci/mapd-core/tree/568e77d9c9706049eeccd32846387f4e042588d0) just after v4.4.2 due to CUDA 10 support)

* [Pandas](https://pandas.pydata.org/) 0.24

* [Postgres](https://www.postgresql.org/) 10-2

* [PDAL](https://pdal.io/) 1.8.0

### Conda Environments

- `requirements_cpu_node.txt` : env where Pandas/Postgres tests were run;

- `requirements_gpu_node.txt`: env where cuDF/MapD Core DB tests were run;

- `requirements_cpu_node_LiDAR-PDAL.txt` : env where PDAL tests on the LiDAR data were run.

### Node hardware specs

- CPU node/worker : 8 vCPUs, ~60GB RAM

- GPU node/worker: V100 GPU, 8 vCPUs, ~60GB RAM

### Dataset Specs

Financial time-series:

- Covering the last 20 years for 63 ETF symbols

- Average per-minute price (available when traded)

- 50 million records

- 3.5 GB CSV file size

- 6 GB in-memory size (RAM, Pandas DF)

- 5 GB in-memory size (GPU memory, cuDF)

---

Geospatial-LiDAR (Montreal LiDAR aerial scan) - stats of one tile:

- 18,306,827 points

- 82 MB Laz

- 1.6 GB CSV

- 681 MB in the mapD DB

### Notes regarding datasets

- Financial data cannot be redistributed due to licensing issues, but a sample is provided to give you an idea regarding its format;

- LiDAR data can be freely downloaded from the website of City of Montreal (link in the code), but if you think your use might entail too many downloads, it's a good idea to contact them to inquire about alternative options (e.g., re-hosting those files in your own infrastructure).

### License

If you use all or part of the code in this repository, we suggest that you include the following notice with your document, code or product:

> This code/product is partly or fully based on the code which was originally run on the UniAnalytica platform (https://www.unianalytica.com) and is published by PatternedScience Inc. at https://github.com/patternedscience/GPU-Analytics-Perf-Tests and licensed under the terms of Apache License 2.0; a copy of the license is available in the GitHub repository.

Feel free to adapt the "*This code/product is partly or fully based on*" part to your situation and use. If you need some modifications to the above text/license to accommodate better your use, please contact [PatternedScience Inc.](https://www.patterned.science/)



Copyright © 2019 PatternedScience Inc.
