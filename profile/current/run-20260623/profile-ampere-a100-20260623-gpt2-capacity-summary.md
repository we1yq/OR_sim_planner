# Runtime-Side Capacity Summary

## LLM

| workload | prompt_len | output_tokens | 1g req/s | 2g req/s | 3g req/s | 4g req/s | 7g req/s |
|---|---|---|---|---|---|---|---|
| gpt2_p1024_o128 | 1024 | 128 | fail | fail | fail | fail | fail |
| gpt2_p1024_o64 | 1024 | 64 | fail | fail | fail | fail | fail |
| gpt2_p512_o128 | 512 | 128 | 0.648 | 0.604 | 0.617 | 0.595 | 0.613 |
| gpt2_p512_o64 | 512 | 64 | 1.009 | 1.128 | 1.015 | 0.957 | 1.051 |
| gpt2_p64_o128 | 64 | 128 | 0.549 | 0.563 | 0.701 | 0.675 | 0.659 |
| gpt2_p64_o64 | 64 | 64 | 1.062 | 1.052 | 1.066 | 0.944 | 0.940 |
