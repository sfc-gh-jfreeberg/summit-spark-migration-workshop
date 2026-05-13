"""Source test for 01_data_ingestion_schema.ipynb entrypoint."""
import importlib
import importlib.util
import sys
from pathlib import Path

_conftest_path=Path(__file__).resolve().parent/"conftest.py"
_spec=importlib.util.spec_from_file_location("_source_conftest",_conftest_path)
_mod=importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
BaseSourceWorkloadTest=_mod.BaseSourceWorkloadTest


class Test01DataIngestionSchema(BaseSourceWorkloadTest):
    """Test suite for the 01_data_ingestion_schema.ipynb source workload."""

    INPUT_FILES = [
        {
            "name": "df:raw_feed_df",
            "full_name": "df:raw_feed_df",
            "source": "01_data_ingestion_schema.ipynb.py:0",
            "type": "file",
            "format": "memory",
            "detection": "static",
            "role": "input",
            "path": "",
            "columns": [
                {
                    "name": "id",
                    "type": "INT",
                    "confidence": "placeholder",
                },
                {
                    "name": "value",
                    "type": "STRING",
                    "confidence": "placeholder",
                },
                {
                    "name": "created_at",
                    "type": "TIMESTAMP",
                    "confidence": "placeholder",
                },
            ],
        },
        {
            "name": "df:result",
            "full_name": "df:result",
            "source": "01_data_ingestion_schema.ipynb.py:0",
            "type": "file",
            "format": "memory",
            "detection": "static",
            "role": "input",
            "path": "",
            "columns": [
                {
                    "name": "client_id",
                    "type": "STRING",
                    "confidence": "evidence",
                },
                {
                    "name": "client_name",
                    "type": "STRING",
                    "confidence": "evidence",
                },
                {
                    "name": "relationship_manager",
                    "type": "STRING",
                    "confidence": "unknown",
                },
                {
                    "name": "tgt_bk_hash",
                    "type": "STRING",
                    "confidence": "unknown",
                },
                {
                    "name": "tgt_t1_hash",
                    "type": "STRING",
                    "confidence": "unknown",
                },
                {
                    "name": "tgt_t2_hash",
                    "type": "STRING",
                    "confidence": "unknown",
                },
            ],
        },
    ]
    INPUT_TABLES = [
        {
            "name": "stg_portfolios",
            "full_name": "stg_portfolios",
            "source": "01_data_ingestion_schema.ipynb.py:0",
            "type": "database",
            "format": "sql",
            "detection": "static",
            "role": "input",
            "path": "",
            "columns": [
                {
                    "name": "portfolio_id",
                    "type": "STRING",
                    "confidence": "pattern",
                },
                {
                    "name": "client_id",
                    "type": "STRING",
                    "confidence": "pattern",
                },
            ],
        },
    ]
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
        source_file=source_dir/"01_data_ingestion_schema.ipynb.py"
        spec=importlib.util.spec_from_file_location("01_data_ingestion_schema_ipynb_py",source_file)
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
