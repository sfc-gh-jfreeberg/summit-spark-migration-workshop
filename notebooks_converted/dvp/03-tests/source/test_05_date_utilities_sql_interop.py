"""Source test for 05_date_utilities_sql_interop.ipynb entrypoint."""
import importlib
import importlib.util
import sys
from pathlib import Path

_conftest_path=Path(__file__).resolve().parent/"conftest.py"
_spec=importlib.util.spec_from_file_location("_source_conftest",_conftest_path)
_mod=importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
BaseSourceWorkloadTest=_mod.BaseSourceWorkloadTest


class Test05DateUtilitiesSqlInterop(BaseSourceWorkloadTest):
    """Test suite for the 05_date_utilities_sql_interop.ipynb source workload."""

    INPUT_FILES = []
    INPUT_TABLES = [
        {
            "name": "PTH_001",
            "full_name": "PTH_001",
            "source": "05_date_utilities_sql_interop.ipynb.py:0",
            "type": "database",
            "format": "sql",
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
            "name": "vw_transactions",
            "full_name": "vw_transactions",
            "source": "05_date_utilities_sql_interop.ipynb.py:0",
            "type": "database",
            "format": "sql",
            "detection": "static",
            "role": "input",
            "path": "",
            "columns": [
                {
                    "name": "portfolio_id",
                    "type": "STRING",
                    "confidence": "evidence",
                },
                {
                    "name": "portfolio_name",
                    "type": "STRING",
                    "confidence": "evidence",
                },
                {
                    "name": "txn_count",
                    "type": "STRING",
                    "confidence": "evidence",
                },
            ],
        },
        {
            "name": "vw_prices",
            "full_name": "vw_prices",
            "source": "05_date_utilities_sql_interop.ipynb.py:0",
            "type": "database",
            "format": "sql",
            "detection": "static",
            "role": "input",
            "path": "",
            "columns": [
                {
                    "name": "month",
                    "type": "INT",
                    "confidence": "pattern",
                },
                {
                    "name": "aapl_close",
                    "type": "STRING",
                    "confidence": "unknown",
                },
                {
                    "name": "msft_close",
                    "type": "STRING",
                    "confidence": "unknown",
                },
                {
                    "name": "aapl_mom_chg",
                    "type": "STRING",
                    "confidence": "unknown",
                },
                {
                    "name": "msft_mom_chg",
                    "type": "STRING",
                    "confidence": "unknown",
                },
            ],
        },
        {
            "name": "vw_monthly_returns_raw",
            "full_name": "vw_monthly_returns_raw",
            "source": "05_date_utilities_sql_interop.ipynb.py:0",
            "type": "database",
            "format": "sql",
            "detection": "static",
            "role": "input",
            "path": "",
            "columns": [
                {
                    "name": "asset_id",
                    "type": "STRING",
                    "confidence": "evidence",
                },
                {
                    "name": "month",
                    "type": "INT",
                    "confidence": "pattern",
                },
                {
                    "name": "close_price",
                    "type": "DECIMAL",
                    "confidence": "evidence",
                },
                {
                    "name": "monthly_return_pct",
                    "type": "DECIMAL",
                    "confidence": "evidence",
                },
            ],
        },
        {
            "name": "vw_ranked_returns",
            "full_name": "vw_ranked_returns",
            "source": "05_date_utilities_sql_interop.ipynb.py:0",
            "type": "database",
            "format": "sql",
            "detection": "static",
            "role": "input",
            "path": "",
            "columns": [
                {
                    "name": "report_month",
                    "type": "INT",
                    "confidence": "pattern",
                },
                {
                    "name": "asset_id",
                    "type": "STRING",
                    "confidence": "pattern",
                },
                {
                    "name": "close_price_usd",
                    "type": "DOUBLE",
                    "confidence": "unknown",
                },
                {
                    "name": "monthly_return",
                    "type": "STRING",
                    "confidence": "unknown",
                },
            ],
        },
        {
            "name": "PTH_002",
            "full_name": "PTH_002",
            "source": "05_date_utilities_sql_interop.ipynb.py:0",
            "type": "database",
            "format": "sql",
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
            "name": "PTH_003",
            "full_name": "PTH_003",
            "source": "05_date_utilities_sql_interop.ipynb.py:0",
            "type": "database",
            "format": "sql",
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
        source_file=source_dir/"05_date_utilities_sql_interop.ipynb.py"
        spec=importlib.util.spec_from_file_location("05_date_utilities_sql_interop_ipynb_py",source_file)
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
