#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import pathlib
import sys


def load_runtime_module():
    path = pathlib.Path(__file__).with_name("torchvision_runtime.py")
    spec = importlib.util.spec_from_file_location("torchvision_runtime", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["torchvision_runtime"] = module
    spec.loader.exec_module(module)
    return module


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--models",
        default="resnet50,resnet101,vgg16,mobilenet_v3_large,efficientnet_b0,vit_b_16,convnext_tiny",
        help="comma-separated torchvision model names to prefetch",
    )
    args = parser.parse_args()

    runtime = load_runtime_module()
    if runtime.IMPORT_ERROR:
        raise RuntimeError(runtime.IMPORT_ERROR)

    for model_name in [item.strip() for item in args.models.split(",") if item.strip()]:
        if model_name not in runtime.MODEL_SPECS:
            raise RuntimeError(f"unsupported torchvision model {model_name!r}")
        factory_name, weights_class_name = runtime.MODEL_SPECS[model_name]
        weights_cls = getattr(runtime.models, weights_class_name)
        factory = getattr(runtime.models, factory_name)
        print(f"prefetching {model_name} ({factory_name}, {weights_class_name}.DEFAULT)", flush=True)
        _ = factory(weights=weights_cls.DEFAULT)


if __name__ == "__main__":
    main()
