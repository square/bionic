import sys
from pathlib import Path

project_path = str(Path("../..").resolve())
if project_path not in sys.path:
    sys.path.insert(0, project_path)
