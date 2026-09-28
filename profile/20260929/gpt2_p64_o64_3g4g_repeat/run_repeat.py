#!/usr/bin/env python3
"""Re-measure gpt2_p64_o64 on 3g/4g: two independent rounds (fresh pod each),
same protocol as ../run_catalog_profile.py.  Usage: run_repeat.py <node> <gpu>..."""
import sys
from pathlib import Path

import run_catalog_profile as rcp

ROUNDS = 2
if __name__ == "__main__":
    node = sys.argv[1]
    for gpu in sys.argv[2:]:
        for rnd in range(1, ROUNDS + 1):
            g = rcp.GPU(node, gpu)
            g.out = rcp.HERE / f"{node}-gpu{gpu}-round{rnd}"
            g.out.mkdir(exist_ok=True)
            g.raw_path = g.out / "raw-samples.csv"
            rcp.run_gpu(g)
