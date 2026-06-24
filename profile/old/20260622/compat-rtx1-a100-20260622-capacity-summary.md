# Runtime-Side Capacity Summary

## VISION

| workload | batch | 1g req/s | 2g req/s | 3g req/s | 4g req/s | 7g req/s |
|---|---|---|---|---|---|---|
| resnet50 | 4 | 168.368 | 166.485 | fail | 164.273 | fail |
| vgg16 | 4 | 170.072 | fail | fail | 430.628 | fail |
| vit_base | 4 | 37.562 | 14.915 | fail | 187.902 | fail |

## LLM

| workload | prompt_len | output_tokens | 1g req/s | 2g req/s | 3g req/s | 4g req/s | 7g req/s |
|---|---|---|---|---|---|---|---|
| gpt2_p64_o64 | 64 | 64 | fail | 0.808 | fail | fail | fail |
| llama32_3b_p64_o64 | 64 | 64 | fail |  |  |  |  |
