# conftest.py
# -----------
# Makes the assignment1/ directory importable when pytest is run from the
# project root or from the tests/ directory.

import sys
import os

# Add the assignment1 directory to sys.path so that split, cooccurrence, etc.
# can be imported directly without a package install.
_assignment1_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "assignment1"))
if _assignment1_dir not in sys.path:
    sys.path.insert(0, _assignment1_dir)