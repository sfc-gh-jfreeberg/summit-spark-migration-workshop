"""SCOS test for 03_aggregations_pivot_windows_scos.ipynb entrypoint."""
import importlib
import importlib.util
import sys
from pathlib import Path

_conftest_path=Path(__file__).resolve().parent/"conftest.py"
_spec=importlib.util.spec_from_file_location("_scos_conftest",_conftest_path)
_mod=importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
BaseScosWorkloadTest=_mod.BaseScosWorkloadTest


class Test03AggregationsPivotWindowsScos(BaseScosWorkloadTest):
    """Test suite for the 03_aggregations_pivot_windows_scos.ipynb SCOS migrated workload."""

    INPUT_FILES = []
    INPUT_TABLES = []
    OUTPUT_FILES = []
    OUTPUT_TABLES = []

    def _call_main(self, session):
        """Import and execute the SCOS migrated workload."""
        for m in list(sys.modules.keys()):
            if m.startswith(("config","utils","pipelines")):del sys.modules[m]
        migrated_dir=Path(__file__).resolve().parent.parent.parent/"02-migrated_scos"
        migrated_file=migrated_dir/"03_aggregations_pivot_windows_scos.ipynb.py"
        spec=importlib.util.spec_from_file_location("03_aggregations_pivot_windows_scos_ipynb_py",migrated_file)
        if spec is None:raise FileNotFoundError(f"Migrated file not found: {migrated_file}")
        mod=importlib.util.module_from_spec(spec)
        sys.modules[spec.name]=mod
        sys.path.insert(0,str(migrated_dir))
        try:
            spec.loader.exec_module(mod)
            f=getattr(mod,"run",None)
            if f is not None:
                r=f(session)
                return r if r is not None else 0
            return 0
        finally:sys.path.pop(0)

    def test_validate_pipeline_runs(self):
        """Confirm the workload executed without error."""
        pass
