# Runtime Profile Benchmark: resnet50

| Metric | Value |
|---|---:|
| client requests | 20 |
| client avg latency ms | 41.775 |
| client p50 latency ms | 40.328 |
| client p95 latency ms | 45.530 |

## Runtime Observations

| Runtime | Profile | Batch | Runtime ms | Runtime throughput | Router avg ms | Network overhead ms | Samples |
|---|---|---:|---:|---:|---:|---:|---:|
| resnet50-ampere-gpu0-s6-7-1g | 1g | 8 | 37.577 | 212.897 | 57.000 | 0.000 | 40 |

## Active Routes

| Runtime | GPU | Profile | Batch | Weight | Runtime ms | Endpoint ms | Network overhead ms |
|---|---|---|---:|---:|---:|---:|---:|
| resnet50-ampere-gpu0-s6-7-1g | ampere-gpu0 | 1g | 8 | 13.441 | 37.577 | 57.000 | 19.423 |

## Profile Catalog

| Profile | Batch | Catalog e2e ms | Catalog mu | Fit SLO |
|---|---:|---:|---:|---|
| 1g | 4 | 13.026 | 307.073 | True |
| 2g | 4 | 6.594 | 606.630 | True |
| 3g | 4 | 5.071 | 788.768 | True |
| 4g | 4 | 4.027 | 993.320 | True |
| 7g | 4 | 3.804 | 1051.414 | True |
| 1g | 8 | 23.973 | 333.709 | True |
| 2g | 8 | 11.934 | 670.359 | True |
| 3g | 8 | 7.066 | 1132.166 | True |
| 4g | 8 | 5.989 | 1335.805 | True |
| 7g | 8 | 3.983 | 2008.385 | True |
| 1g | 16 | 45.580 | 351.034 | True |
| 2g | 16 | 22.780 | 702.364 | True |
| 3g | 16 | 13.063 | 1224.843 | True |
| 4g | 16 | 11.196 | 1429.107 | True |
| 7g | 16 | 6.806 | 2350.694 | True |
| 1g | 32 | 85.445 | 0.000 | False |
| 2g | 32 | 42.834 | 747.077 | True |
| 3g | 32 | 24.347 | 1314.314 | True |
| 4g | 32 | 21.461 | 1491.105 | True |
| 7g | 32 | 12.383 | 2584.209 | True |
| 1g | 64 | 164.529 | 0.000 | False |
| 2g | 64 | 82.188 | 0.000 | False |
| 3g | 64 | 46.408 | 1379.073 | True |
| 4g | 64 | 41.511 | 1541.745 | True |
| 7g | 64 | 23.367 | 2738.905 | True |
| 1g | 128 | 323.500 | 0.000 | False |
| 2g | 128 | 161.015 | 0.000 | False |
| 3g | 128 | 90.112 | 0.000 | False |
| 4g | 128 | 81.159 | 0.000 | False |
| 7g | 128 | 44.631 | 2867.975 | True |
