# Runtime-Side Capacity Summary

## VISION

| workload | batch | 1g req/s | 2g req/s | 3g req/s | 4g req/s | 7g req/s |
|---|---|---|---|---|---|---|
| resnet50 | 4 | 157.577 | 32.326 | 157.249 | 155.236 | 87.908 |
| vgg16 | 4 | 169.893 | 312.157 | 360.271 | 363.113 | 377.250 |
| vit_base | 4 | 54.628 | 104.336 | 146.465 | 192.827 | 291.445 |

## LLM

| workload | prompt_len | output_tokens | 1g req/s | 2g req/s | 3g req/s | 4g req/s | 7g req/s |
|---|---|---|---|---|---|---|---|
| gpt2_p64_o64 | 64 | 64 | 1.429 | 1.488 | 1.113 | 1.455 | 1.478 |
