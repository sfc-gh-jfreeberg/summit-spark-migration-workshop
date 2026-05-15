"""Source test for 04_functional_pipeline.ipynb entrypoint."""
import importlib
import importlib.util
import sys
from pathlib import Path

_conftest_path=Path(__file__).resolve().parent/"conftest.py"
_spec=importlib.util.spec_from_file_location("_source_conftest",_conftest_path)
_mod=importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
BaseSourceWorkloadTest=_mod.BaseSourceWorkloadTest


class Test04FunctionalPipeline(BaseSourceWorkloadTest):
    """Test suite for the 04_functional_pipeline.ipynb source workload."""

    INPUT_FILES = []
    INPUT_TABLES = []
    OUTPUT_FILES = []
    OUTPUT_TABLES = []

    def _call_main(self, session):
        """Import and execute the source workload."""
        import os
        input_path=os.environ.get("INPUT_DATA_PATH",".")
        output_dir=str(Path(input_path).parent/"output")
        os.environ["OUTPUT_DATA_PATH"]=output_dir
        os.makedirs(output_dir,exist_ok=True)
        for m in list(sys.modules.keys()):
            if m.startswith(("config","utils","pipelines")):del sys.modules[m]
        source_dir=Path(__file__).resolve().parent.parent.parent/"01-source"
        source_file=source_dir/"04_functional_pipeline.ipynb.py"
        spec=importlib.util.spec_from_file_location("04_functional_pipeline_ipynb_py",source_file)
        if spec is None:raise FileNotFoundError(f"Source file not found: {source_file}")
        mod=importlib.util.module_from_spec(spec)
        sys.modules[spec.name]=mod
        sys.path.insert(0,str(source_dir))
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
