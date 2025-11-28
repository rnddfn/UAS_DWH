#!/usr/bin/env python3
"""
pipeline.py
Simple runner: Extract -> Transform -> Load
Assumes you have: extract.py, transform.py, load.py in repo.
This script will prefer to import functions if available;
otherwise it will call scripts via subprocess.
"""

import os
import logging
import subprocess
import importlib
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def run_module_function(module_name: str, func_name: str):
    try:
        m = importlib.import_module(module_name)
        func = getattr(m, func_name)
        logging.info("Running %s.%s()", module_name, func_name)
        func()
        return True
    except Exception as e:
        logging.warning("Import/Run failed for %s.%s: %s", module_name, func_name, e)
        return False

def run_script(script_path: str, args=None):
    cmd = [sys.executable, script_path]
    if args:
        cmd += args
    logging.info("Running script: %s", " ".join(cmd))
    res = subprocess.run(cmd, check=False)
    if res.returncode != 0:
        logging.error("Script %s failed with exit code %s", script_path, res.returncode)
        raise SystemExit(res.returncode)

def main():
    # 1) Extract
    if not run_module_function("extract", "run_extract"):
        # fallback to running script
        run_script("extract.py")

    # 2) Transform
    if not run_module_function("transform", "run_transform"):
        run_script("transform.py")

    # 3) Load
    try:
        import load
        logging.info("Calling load.run_load()")
        load.run_load(truncate_facts=False)
    except Exception:
        # fallback: call script
        run_script("load.py")

    logging.info("Pipeline finished successfully.")

if __name__ == "__main__":
    main()