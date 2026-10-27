"""Static checks for the reconciliation notebook (the full run happens in the local-e2e CI job)."""
import ast
import json
import subprocess
import sys
from pathlib import Path

NB_DIR = Path(__file__).resolve().parents[1]
NB = NB_DIR / "pipeline_reconciliation.ipynb"


def load():
    return json.loads(NB.read_text())


def test_notebook_is_in_sync_with_its_source(tmp_path):
    """The committed .ipynb must be what build_notebook.py generates."""
    before = NB.read_text()
    subprocess.run([sys.executable, str(NB_DIR / "build_notebook.py")], check=True, capture_output=True)
    assert NB.read_text() == before, "run `python notebooks/build_notebook.py` and commit the result"


def test_has_papermill_parameters_cell():
    params = [c for c in load()["cells"] if "parameters" in c.get("metadata", {}).get("tags", [])]
    assert len(params) == 1
    src = "".join(params[0]["source"])
    for name in ("PLATFORM", "ENV", "BUSINESS_DATE", "WRITE_RESULTS", "FAIL_ON_BREAK"):
        assert f"{name} =" in src


def test_code_cells_are_valid_python():
    for cell in load()["cells"]:
        if cell["cell_type"] == "code":
            ast.parse("".join(cell["source"]))


def test_ships_without_outputs():
    assert all(not c.get("outputs") for c in load()["cells"] if c["cell_type"] == "code")
