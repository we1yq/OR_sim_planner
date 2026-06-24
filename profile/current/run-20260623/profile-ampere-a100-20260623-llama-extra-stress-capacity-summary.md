# Runtime-Side Capacity Summary

## LLM

| workload | prompt_len | output_tokens | 1g req/s | 2g req/s | 3g req/s | 4g req/s | 7g req/s |
|---|---|---|---|---|---|---|---|
| llama32_3b_p16384_o128 | 16384 | 128 |  | fail | 0.105 | 0.125 | 0.189 |
| llama32_3b_p16384_o512 | 16384 | 512 |  | fail | 0.030 | 0.036 | 0.055 |
| llama32_3b_p8192_o128 | 8192 | 128 |  | 0.112 | 0.178 | 0.204 | 0.288 |
| llama32_3b_p8192_o512 | 8192 | 512 |  | 0.031 | 0.049 | 0.056 | 0.081 |
