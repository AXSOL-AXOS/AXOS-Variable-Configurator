import os
import sys

# Ensure repo root is on sys.path for direct script imports in tests.
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
