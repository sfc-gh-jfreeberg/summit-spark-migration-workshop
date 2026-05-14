# UDF Dependencies in SCOS

When SCOS executes UDFs (`@udf`, `@pandas_udf`, `applyInPandas`, `mapInPandas`), the function is serialized with **cloudpickle** and sent to Snowflake's server-side Python worker. Two types of `ModuleNotFoundError` can occur:

1. **Package not available** — The UDF imports a third-party package (e.g., `numpy`, `scikit-learn`) that isn't on the server
2. **Custom code not available** — The UDF calls helper functions or imports custom modules that don't exist on the server

This guide covers how to resolve both.

---

## Quick Reference: Config Keys

| Config Key | Purpose |
|---|---|
| `snowpark.connect.udf.packages` | Packages to install: `[pkg1, pkg2]` |
| `snowpark.connect.artifact_repository` | Package source: `snowflake.snowpark.pypi_shared_repository` (defaults to Anaconda if not set) |
| `snowpark.connect.udf.python.imports` | Python files/zips: `[@stage/file.py, local/path.py]` |
| `snowpark.connect.udf.java.imports` | Java JARs for Java UDFs |

---

# Part 1: Third-Party Packages

By default, the server-side worker only has access to packages from **Snowflake's Anaconda channel**. You can also use **PyPI** via an artifact repository.

## Checking Anaconda Availability

```sql
SELECT PACKAGE_NAME, VERSION
FROM INFORMATION_SCHEMA.PACKAGES
WHERE LANGUAGE = 'python'
  AND PACKAGE_NAME ILIKE '%<package_name>%'
ORDER BY PACKAGE_NAME;
```

Common packages that **ARE** available: `numpy`, `pandas`, `scikit-learn`, `scipy`, `nltk`, `xgboost`, `lightgbm`, `statsmodels`, `pyarrow`, `cachetools`.

---

## Declaring Packages

### Option 1: Anaconda Channel (default)

If the package is in the Anaconda channel:

```python
spark.conf.set("snowpark.connect.udf.packages", "[numpy, scikit-learn]")
```

### Option 2: PyPI via Artifact Repository

If the package is **not** in Anaconda but is available on PyPI:

```python
# Use PyPI shared repository instead of Anaconda channel
spark.conf.set("snowpark.connect.artifact_repository", "snowflake.snowpark.pypi_shared_repository")

# Declare packages (resolved from PyPI)
spark.conf.set("snowpark.connect.udf.packages", "[numpy, scikit-learn, some-pypi-only-package]")
```

**Note:** `snowpark.connect.udf.packages` specifies **which** packages. `snowpark.connect.artifact_repository` specifies **where** to resolve them from.

Then import **inside** the UDF body:

```python
@udf(returnType=DoubleType())
def predict(features: list) -> float:
    from sklearn.linear_model import LinearRegression
    import numpy as np
    # ... model logic ...
```

---

## When a Package Is NOT Available Anywhere

### Option A: Use PyPI (Recommended)

```python
spark.conf.set("snowpark.connect.artifact_repository", "snowflake.snowpark.pypi_shared_repository")
spark.conf.set("snowpark.connect.udf.packages", "[your-package-name]")
```

### Option B: Replace with stdlib/numpy equivalent

Write a pure-Python or numpy-only implementation. Example — replacing `pykalman.KalmanFilter`:

```python
import numpy as np

def _kalman_smooth_1d(series, initial_state_mean):
    """Minimal 1D Kalman smoother (forward-backward) using only numpy."""
    n = len(series)
    if n == 0:
        return np.array([]).reshape(-1, 1)

    Q = 0.01   # transition (process) noise covariance
    R = 1.0    # observation noise covariance

    # Forward pass (filter)
    x_filt = np.zeros(n)
    P_filt = np.zeros(n)
    x_pred = np.zeros(n)
    P_pred = np.zeros(n)

    x_pred[0] = initial_state_mean
    P_pred[0] = 1.0

    for t in range(n):
        K = P_pred[t] / (P_pred[t] + R)
        x_filt[t] = x_pred[t] + K * (series[t] - x_pred[t])
        P_filt[t] = (1 - K) * P_pred[t]
        if t < n - 1:
            x_pred[t + 1] = x_filt[t]
            P_pred[t + 1] = P_filt[t] + Q

    # Backward pass (smoother)
    x_smooth = np.zeros(n)
    x_smooth[-1] = x_filt[-1]
    for t in range(n - 2, -1, -1):
        L = P_filt[t] / P_pred[t + 1]
        x_smooth[t] = x_filt[t] + L * (x_smooth[t + 1] - x_pred[t + 1])

    return x_smooth.reshape(-1, 1)
```

### Option C: Upload a pure-Python wheel

If the package is pure Python (no C extensions):

```python
spark.conf.set("snowpark.connect.udf.python.imports", "[@stage/my_package.zip]")
```

This only works for packages with no compiled dependencies.

---

## Common Missing Packages and Replacements

| Missing Package | Replacement |
|---|---|
| `pykalman` | numpy-only Kalman filter (see example above) |
| `boto3` / `botocore` | Not needed — use Snowflake stages for data access |
| `pyspark.ml.*` | Snowpark ML or scikit-learn |
| `dbutils` | `os.environ` / `sys.argv` for parameters |
| `delta.tables` | Not applicable — Delta format not supported |

---

# Part 2: Custom Code & Helper Functions

When cloudpickle serializes a UDF, it may reference modules that don't exist on the server. Three tiers of fixes, ordered by preference:

## Tier 1 (Recommended): SCOS Import Configs

### Third-party packages

```python
spark.conf.set("snowpark.connect.udf.packages", "[numpy, pandas, scikit-learn]")

@udf(returnType=IntegerType())
def square(v: int) -> int:
    import numpy as np  # import INSIDE the UDF
    return int(np.square(np.array([v]))[0])
```

### Custom Python modules

Upload your module to a Snowflake stage, then register it:

```python
from snowflake.snowpark import Session

# Upload helper module to session stage
snowpark_session = Session.builder.config("connection_name", "default").create()
snowpark_session.file.put(
    "path/to/my_helpers.py",
    snowpark_session.get_session_stage(),
    auto_compress=False,
)

# Register for UDF imports
spark.conf.set(
    "snowpark.connect.udf.python.imports",
    f"[{snowpark_session.get_session_stage()}/my_helpers.py]",
)

@udf(returnType=StringType())
def process(input: str) -> str:
    from my_helpers import transform  # available on server via imports config
    return transform(input)
```

---

## Tier 2 (Inline): Self-contained UDF Functions

For simple UDFs, keep all logic **self-contained** — no calls to external helper functions:

```python
def normalize(pdf):
    v = pdf.v
    return pdf.assign(v=(v - v.mean()) / v.std())

df.groupby("id").applyInPandas(normalize, schema="id long, v double")
```

When the UDF is simple enough that all logic fits in one function, cloudpickle serializes it by value with no module reference issues.

**When to use:**
- UDF logic is straightforward (normalization, aggregation, filtering)
- No calls to helper functions defined elsewhere
- No imports of custom modules

---

## Tier 3 (Workaround): `__module__` Patching + Factory Functions

When the UDF calls **multiple helper functions** in the same file, and refactoring inline is impractical:

### Problem

cloudpickle serializes functions **by module reference** when `func.__module__` points to an importable module (e.g., `my_pipeline_scos`). The server tries to `import my_pipeline_scos`, which doesn't exist.

### Fix Part A: Factory function for captured data

If the UDF references **module-level globals**, wrap it in a factory function:

```python
# BEFORE (broken): module-level global reference
broadcast_thresholds = {...}  # module global

def denoise_group_udf(pdf):
    # References broadcast_thresholds from module scope — fails on server
    group = detect_breakpoint(group, broadcast_thresholds)
    return group

# AFTER (fixed): factory function captures data by value
def make_denoise_udf(thresholds):
    """Factory: captures thresholds in closure."""
    def denoise_group_udf(pdf):
        group = detect_breakpoint(group, thresholds)  # from closure, not module
        return group
    return denoise_group_udf

denoise_udf = make_denoise_udf(roofmateri_thresholds_dict)
```

### Fix Part B: `__module__` patching

Patch `__module__ = "__main__"` on the UDF **and every helper function it calls**:

```python
# Patch ALL functions in the UDF's call chain
for _fn in [denoise_udf, smooth_adra, detect_breakpoint,
            compute_ADRA_smoothed_reset, compute_confidence,
            merge_close_dates, _kalman_smooth_1d]:
    _fn.__module__ = "__main__"

# Now applyInPandas works — all functions serialized by value
result_df = df.groupby("key").applyInPandas(denoise_udf, schema=output_schema)
```

**When to use:**
- UDF calls many helper functions defined in the same file
- Helpers are tightly coupled (call each other in a chain)
- Refactoring inline would duplicate too much code
- Uploading as a separate module (Tier 1) is impractical

**Important:** Patch **every** function in the transitive call chain, not just the top-level UDF.

---

## Decision Flowchart

```
UDF needs external code?
├── No → Tier 2: keep it self-contained (inline)
└── Yes
    ├── Code is in a separate .py file?
    │   └── Yes → Tier 1: upload to stage + snowpark.connect.udf.python.imports
    ├── Code needs third-party package?
    │   └── Yes → snowpark.connect.udf.packages
    │            ├── In Anaconda channel? → Use default
    │            └── Not in Anaconda? → Set artifact_repository to pypi_shared_repository
    │                                   OR replace with stdlib/numpy equivalent
    └── Code is helper functions in the same workload file?
        └── Yes → Tier 3: factory function + __module__ patching
```
