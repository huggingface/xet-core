# -*- coding: utf-8 -*-
from importlib.metadata import version as _version, PackageNotFoundError
from packaging.version import Version as _Version
import warnings

def get_hfhub_version() -> str:
    try:
        return _version("huggingface_hub")
    except importlib.metadata.PackageNotFoundError:
        # package is not installed
        return "0.0.0"

if _Version(get_hfhub_version()) < _Version("0.30.0"):
    warnings.warn(
        "To use hf_xet, huggingface_hub >= 0.30.0 is required. "
        "Please update to the latest version for full functionality.",
        UserWarning,
    )