# Runtime-Side Capacity Summary

## VISION

| workload | batch | 1g req/s | 2g req/s | 3g req/s | 4g req/s | 7g req/s |
|---|---|---|---|---|---|---|
| resnet50 | 4 |  |  |  |  | fail |
| vgg16 | 4 |  |  |  |  | 18.085 |
| vit_base | 4 |  |  |  |  | 36.109 |

## LLM

| workload | prompt_len | output_tokens | 1g req/s | 2g req/s | 3g req/s | 4g req/s | 7g req/s |
|---|---|---|---|---|---|---|---|
| gpt2_p64_o64 | 64 | 64 |  |  |  |  | fail |
| llama32_3b_p64_o64 | 64 | 64 |  |  |  |  | fail |
