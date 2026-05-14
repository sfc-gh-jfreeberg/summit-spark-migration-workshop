# flake8: noqa: T201

"""
SCOS Migration Agent - PySpark Compatibility Analyzer

Analyze PySpark scripts for potential SCOS compatibility issues.

Usage:
    python analyze_pyspark.py --path /path/to/script.py
    python analyze_pyspark.py --path /path/to/scripts/

This script:
1. Parses PySpark files using Python AST (handles multi-line statements)
2. Extracts complete SQL expressions and method chains
3. Checks API compatibility from the compatibility CSV
4. Uses unified RAG to find similar failing SQL and DataFrame patterns
5. Reports results with root causes and workarounds
"""

import argparse
import ast
import csv
import json
import logging
import re
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from code_normalization import normalize_code_lightweight
from rag import BaseRAG
from scos_session import add_connectivity_args, build_rag, open_session
from snowflake.cortex import CompleteOptions, complete as cortex_complete

from snowflake.snowpark import Session

logger = logging.getLogger(__name__)


def extract_notebook_code(ipynb_path: str) -> list[dict]:
    """Extract code cells from a Jupyter notebook for analysis.

    Returns list of dicts with keys: cell_index, source, line_offset
    """
    import json
    with open(ipynb_path) as f:
        nb = json.load(f)

    code_cells = []
    for i, cell in enumerate(nb.get("cells", [])):
        if cell.get("cell_type") == "code":
            source = cell.get("source", [])
            if isinstance(source, list):
                source = "".join(source)
            code_cells.append({
                "cell_index": i,
                "source": source,
                "line_offset": 0,
            })
    return code_cells

# LLM model for validation
DEFAULT_LLM_MODEL = "claude-opus-4-5"

# Batch LLM validation prompt - analyzes multiple code blocks at once
PROMPT_PREDICT_COMPATIBILITY_BATCH = """
You are analyzing multiple PySpark code blocks for compatibility issues when running on Snowflake SCOS (Snowpark Connect for Spark).
Your goal is to analyze each code block and determine if it will actually fail on SCOS.

## INPUT DATA
You are provided with {num_blocks} code blocks. Each block contains:
1. `block_id`: Unique identifier.
2. `input_code`: The PySpark code snippet to analyze.
3. `preliminary_assessment`: Rule-based warnings (e.g., "API X is unsupported").
4. `matching_patterns`: Similar failing test cases from our database.

## ANALYSIS PROCESS (Apply to EACH block)
1. **Analyze Input**: Understand the intent and syntax of the `input_code`.
2. **Verify RAG Matches**: Compare `input_code` with `matching_patterns`.
   - **Crucial**: Do the failing patterns share the *exact same* root cause as the input?
   - *Example*: If the RAG shows a failure for `.write.format("avro")` but your input is `.write.format("parquet")`, this is a **FALSE POSITIVE**. The risk is LOW.
3. **Verify Rule-Based Warnings**: Check if the `preliminary_assessment` is valid or a false alarm (e.g., `hint()` is just a no-op).

## IMPORTANT RULES FOR RISK SCORING:
- If the similar test cases use DIFFERENT operations/patterns that don't apply to the input code → final_risk should be 0.0 to 0.1
- If there are NO compatibility issues with the input code → final_risk should be 0.0
- If the similar test cases use the SAME problematic pattern as the input code → final_risk should be 0.5 to 1.0
- Only assign high risk (>0.5) if you're confident the input code will ACTUALLY fail for the SAME reason as the similar test cases
- If there are no similar test cases, but the `SCOS Issues Risk` score exists and is above 0, use it as the `final_risk` score.

BE CONSISTENT: If your explanation says "should work correctly" or "issues don't apply", then final_risk MUST be < 0.1

## CODE BLOCKS TO ANALYZE

{code_blocks_text}

## OUTPUT FORMAT
Return ONLY a valid JSON array with EXACTLY {num_blocks} items (one for each code block, in order).
Your response must contain NO text before or after the JSON array.

[
    {{
        "block_id": "<the block_id from the input>",
        "analysis_thought_process": "<Step-by-step reasoning: 1. Input does X. 2. Compare with preliminary assessment and similar test cases. 3. Conclusion.>"
        "final_risk": <0.0-1.0 float - probability of a failure>,
        "root_cause": "<Actual root cause of failure, or null if safe>",
        "explanation": "<Concise summary (1-2 sentences) for the user explaining your assessment>",
        "fix": "<specific fix/workaround if needed, or null if code is fine>",
        "confidence": "<HIGH|MEDIUM|LOW>"
    }},
    ...
]
"""

# Default batch size for LLM calls
DEFAULT_LLM_BATCH_SIZE = 5

DATA_DIR = Path(__file__).parent / "data"

# SNOW-3347480: Safe-API allowlist — APIs that need no RAG lookup
_SAFE_APIS: set[str] | None = None
_SAFE_API_SKIPS: int = 0  # Counter for skipped queries


def load_safe_apis(json_path: Path | None = None) -> set[str]:
    """
    Load the safe-API allowlist from JSON.

    Returns a set of API pattern strings that are confirmed fully compatible
    with Spark Connect and require no RAG query.

    Falls back to empty set (all APIs queried) if file is missing.
    """
    if json_path is None:
        json_path = DATA_DIR / "safe_apis.json"
    if not json_path.exists():
        logger.warning("Safe-API allowlist not found at %s — all APIs will be queried", json_path)
        return set()
    try:
        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)
        apis = {entry["pattern"] for entry in data.get("apis", [])}
        logger.info("Loaded %d safe-API patterns from %s", len(apis), json_path.name)
        return apis
    except (json.JSONDecodeError, KeyError) as exc:
        logger.warning("Failed to parse safe-API allowlist %s: %s — all APIs will be queried", json_path, exc)
        return set()


def is_block_safe(block_functions: list[str], safe_apis: set[str]) -> bool:
    """
    Check if ALL functions in a code block are in the safe-API allowlist.

    SNOW-3347480: If every function in the block is known-safe, we skip
    the RAG query entirely for this block.
    """
    if not safe_apis or not block_functions:
        return False
    return all(func in safe_apis for func in block_functions)


# Compatibility scores (0-1 scale)
COMPAT_SCORES = {
    "D0": 1.0,
    "D1": 0.8,
    "D2": 0.5,
    "NONE": 0.0,
    "UNKNOWN": None,
    "OUTOFSCOPE": 0.0,
}

# SNOW-3347695: Per-property SparkContext replacement table with risk scores and static fallbacks
SPARK_CONTEXT_PROPERTIES = {
    "master": {
        "risk": 0.4,
        "replacement": '"sc://" + os.environ.get("SPARK_CONNECT_URL", "local")',
        "reason": "sparkContext.master is not available in Spark Connect. Replace with static string for diagnostic logging.",
        "category": "SparkContext Property",
    },
    "applicationId": {
        "risk": 0.2,
        "replacement": 'spark.conf.get("spark.app.id", "unknown")',
        "reason": "sparkContext.applicationId is not available in Spark Connect. Use spark.conf.get() instead.",
        "category": "SparkContext Property",
    },
    "appName": {
        "risk": 0.2,
        "replacement": 'spark.conf.get("spark.app.name", "unknown")',
        "reason": "sparkContext.appName is not available in Spark Connect. Use spark.conf.get() instead.",
        "category": "SparkContext Property",
    },
    "getConf": {
        "risk": 0.3,
        "replacement": "spark.conf",
        "reason": "sparkContext.getConf is not available in Spark Connect. Use spark.conf.get(key) / spark.conf.getAll instead.",
        "category": "SparkContext Property",
    },
    "statusTracker": {
        "risk": 0.6,
        "replacement": 'int(os.environ.get("SPARK_WORKER_NODES", "1"))',
        "reason": "sparkContext.statusTracker is not available in Spark Connect. No equivalent — use environment variables.",
        "category": "SparkContext Property",
    },
    "_jvm": {
        "risk": 1.0,
        "replacement": None,
        "reason": "sparkContext._jvm is not available in Spark Connect. Hard blocker — requires full rewrite to cloud-native API.",
        "category": "SparkContext Property",
    },
    "_jsc": {
        "risk": 1.0,
        "replacement": None,
        "reason": "sparkContext._jsc is not available in Spark Connect. Hard blocker — requires full rewrite to cloud-native API.",
        "category": "SparkContext Property",
    },
    "hadoopConfiguration": {
        "risk": 1.0,
        "replacement": None,
        "reason": "sparkContext.hadoopConfiguration is not available in Spark Connect. Use Snowflake storage integration for credentials, boto3/stage for filesystem access.",
        "category": "SparkContext Property",
    },
    "parallelize": {
        "risk": 0.8,
        "replacement": "spark.createDataFrame()",
        "reason": "sparkContext.parallelize is not available in Spark Connect. Use spark.createDataFrame() instead.",
        "category": "SparkContext Property",
    },
    "textFile": {
        "risk": 0.8,
        "replacement": "spark.read.text()",
        "reason": "sparkContext.textFile is not available in Spark Connect. Use spark.read.text() with stage-based path.",
        "category": "SparkContext Property",
    },
    "broadcast": {
        "risk": 0.7,
        "replacement": None,
        "reason": "sparkContext.broadcast is not available in Spark Connect. Use DataFrame join hints (broadcast(df)) or pass lookup data as regular variables for small datasets.",
        "category": "SparkContext Property",
    },
    "accumulator": {
        "risk": 0.7,
        "replacement": None,
        "reason": "sparkContext.accumulator is not available in Spark Connect. Use DataFrame aggregations or external counters.",
        "category": "SparkContext Property",
    },
    "version": {
        "risk": 0.1,
        "replacement": "spark.version",
        "reason": "sparkContext.version is not available in Spark Connect. Use spark.version instead.",
        "category": "SparkContext Property",
    },
    "defaultParallelism": {
        "risk": 0.3,
        "replacement": 'int(os.environ.get("SPARK_DEFAULT_PARALLELISM", "200"))',
        "reason": "sparkContext.defaultParallelism is not available in Spark Connect. Use environment variable or default.",
        "category": "SparkContext Property",
    },
    "defaultMinPartitions": {
        "risk": 0.3,
        "replacement": 'int(os.environ.get("SPARK_MIN_PARTITIONS", "2"))',
        "reason": "sparkContext.defaultMinPartitions is not available in Spark Connect. Use environment variable or default.",
        "category": "SparkContext Property",
    },
    "uiWebUrl": {
        "risk": 0.3,
        "replacement": '"N/A — Spark Connect mode"',
        "reason": "sparkContext.uiWebUrl is not available in Spark Connect. Replace with static string.",
        "category": "SparkContext Property",
    },
}

# SNOW-3347699: Hadoop filesystem access patterns
HADOOP_PATTERNS = {
    "FileSystem.get": {
        "risk": 1.0,
        "reason": "Hadoop FileSystem.get() requires JVM interop (SparkContext._jvm) not available in Spark Connect. Replace with boto3/azure-storage-blob/google-cloud-storage.",
        "category": "Hadoop Filesystem",
        "how_to_fix": "Replace with cloud-native SDK: boto3 for S3, azure-storage-blob for ABFS, google-cloud-storage for GCS, or Snowflake stage operations.",
    },
    "hadoop.fs.Path": {
        "risk": 1.0,
        "reason": "Hadoop Path operations require JVM interop not available in Spark Connect.",
        "category": "Hadoop Filesystem",
        "how_to_fix": "Use Python pathlib or cloud-native SDK for path operations.",
    },
    "hadoopConfiguration().set": {
        "risk": 1.0,
        "reason": "Hadoop configuration for cloud credentials is not available in Spark Connect. Use Snowflake storage integration instead.",
        "category": "Hadoop Filesystem",
        "how_to_fix": "Create a Snowflake storage integration for the cloud provider. See: CREATE STORAGE INTEGRATION.",
    },
}

# SNOW-3347699: DBFS path patterns
DBFS_PATH_PATTERNS = [
    "dbfs:/",
    "dbfs:",
    "/mnt/",
]

# SNOW-3347690: USE DATABASE/SCHEMA statement patterns
USE_STATEMENT_PATTERNS = [
    r"""spark\.sql\s*\(\s*["']USE\s+DATABASE\s+""",
    r"""spark\.sql\s*\(\s*["']USE\s+SCHEMA\s+""",
    r"""spark\.sql\s*\(\s*["']USE\s+""",
    r"""\.catalog\.setCurrentDatabase\s*\(""",
]

# SNOW-3347693: JVM-only library imports that won't work in Spark Connect
JVM_ONLY_IMPORTS = {
    "pydeequ": {
        "risk": 1.0,
        "reason": "pydeequ requires JVM interop (Amazon Deequ) not available in Spark Connect. Replace with native DataFrame validation.",
        "category": "JVM Library",
        "how_to_fix": (
            "Replace Deequ checks with native DataFrame equivalents: "
            "isComplete → filter(col.isNull()).count(), "
            "isUnique → groupBy(col).count().filter(count > 1), "
            "isNonNegative → filter(col < 0).count(), "
            "hasCompleteness → (total - nulls) / total >= threshold."
        ),
    },
    "great_expectations.dataset.sparkdf_dataset": {
        "risk": 1.0,
        "reason": "Great Expectations SparkDFDataset requires SparkContext not available in Spark Connect.",
        "category": "JVM Library",
        "how_to_fix": "Use Great Expectations with PandasDataset or SqlAlchemyDataset, or use native DataFrame validation.",
    },
    "com.amazon.deequ": {
        "risk": 1.0,
        "reason": "Amazon Deequ is a JVM-only library not available in Spark Connect.",
        "category": "JVM Library",
        "how_to_fix": "Replace with native DataFrame validation operations.",
    },
    "com.holdenkarau.spark.testing": {
        "risk": 1.0,
        "reason": "Spark Testing Base requires SparkContext not available in Spark Connect.",
        "category": "JVM Library",
        "how_to_fix": "Use pytest with SparkSession.builder for testing, or mock-based testing approaches.",
    },
}

# SNOW-3319134: ML pipeline patterns (pyspark.ml imports and classes)
ML_PIPELINE_PATTERNS = {
    "LogisticRegression": {
        "risk": 1.0,
        "reason": "pyspark.ml.classification.LogisticRegression is not supported in SCOS. Use snowflake.ml.modeling.linear_model.LogisticRegression.",
        "category": "ML Pipeline",
        "how_to_fix": "Replace with snowflake.ml.modeling.linear_model.LogisticRegression. Rename params: maxIter→max_iter, regParam→C (C=1/regParam), featuresCol→input_cols (list), labelCol→label_cols (list). Add output_cols.",
    },
    "RandomForestClassifier": {
        "risk": 1.0,
        "reason": "pyspark.ml.classification.RandomForestClassifier is not supported in SCOS. Use snowflake.ml.modeling.ensemble.RandomForestClassifier.",
        "category": "ML Pipeline",
        "how_to_fix": "Replace with snowflake.ml.modeling.ensemble.RandomForestClassifier. Rename: numTrees→n_estimators, maxDepth→max_depth, featuresCol→input_cols, labelCol→label_cols.",
    },
    "GBTClassifier": {
        "risk": 1.0,
        "reason": "pyspark.ml.classification.GBTClassifier is not supported in SCOS. Use snowflake.ml.modeling.ensemble.GradientBoostingClassifier.",
        "category": "ML Pipeline",
        "how_to_fix": "Replace with snowflake.ml.modeling.ensemble.GradientBoostingClassifier. Rename: maxIter→n_estimators, maxDepth→max_depth.",
    },
    "RandomForestRegressor": {
        "risk": 1.0,
        "reason": "pyspark.ml.regression.RandomForestRegressor is not supported in SCOS. Use snowflake.ml.modeling.ensemble.RandomForestRegressor.",
        "category": "ML Pipeline",
        "how_to_fix": "Replace with snowflake.ml.modeling.ensemble.RandomForestRegressor. Rename: numTrees→n_estimators, maxDepth→max_depth.",
    },
    "LinearRegression": {
        "risk": 1.0,
        "reason": "pyspark.ml.regression.LinearRegression is not supported in SCOS. Use snowflake.ml.modeling.linear_model.LinearRegression.",
        "category": "ML Pipeline",
        "how_to_fix": "Replace with snowflake.ml.modeling.linear_model.LinearRegression. Rename: maxIter→max_iter, regParam→alpha, featuresCol→input_cols, labelCol→label_cols.",
    },
    "Pipeline": {
        "risk": 1.0,
        "reason": "pyspark.ml.Pipeline is not supported in SCOS. Use snowflake.ml.modeling.pipeline.Pipeline or sequential fit/predict calls.",
        "category": "ML Pipeline",
        "how_to_fix": "Replace with snowflake.ml.modeling.pipeline.Pipeline or call fit/predict on each stage sequentially.",
    },
    "CrossValidator": {
        "risk": 1.0,
        "reason": "pyspark.ml.tuning.CrossValidator is not supported in SCOS. Use snowflake.ml.modeling.model_selection.GridSearchCV.",
        "category": "ML Pipeline",
        "how_to_fix": "Replace with snowflake.ml.modeling.model_selection.GridSearchCV. Rename: estimator→estimator, numFolds→cv, estimatorParamMaps→param_grid.",
    },
    "VectorAssembler": {
        "risk": 1.0,
        "reason": "pyspark.ml.feature.VectorAssembler is not needed in Snowflake ML. Snowflake ML accepts multiple input columns directly.",
        "category": "ML Pipeline",
        "how_to_fix": "Remove VectorAssembler. Pass the original feature columns directly to the estimator via input_cols=[col1, col2, ...].",
    },
}

# SNOW-3319139: UDTF/UDAF patterns
UDTF_UDAF_PATTERNS = {
    "@udtf": {
        "risk": 0.8,
        "reason": "PySpark @udtf decorator needs structural transformation to Snowpark UDTF handler class with process()/endPartition() methods.",
        "category": "UDTF/UDAF",
        "how_to_fix": "Convert to Snowpark UDTF handler: rename eval()→process(), add endPartition()→yield, register with session.udtf.register().",
    },
    "PandasUDFType.GROUPED_AGG": {
        "risk": 0.8,
        "reason": "PandasUDFType.GROUPED_AGG needs conversion to Snowpark vectorized UDAF with accumulate/merge/finish pattern.",
        "category": "UDTF/UDAF",
        "how_to_fix": "Convert to Snowpark UDAF: create handler class with accumulate(), merge(), finish() methods. Register with session.udaf.register().",
    },
    "PandasUDFType.SCALAR": {
        "risk": 0.5,
        "reason": "PandasUDFType.SCALAR can be simplified to @udf with pandas Series type hints in Spark Connect.",
        "category": "UDTF/UDAF",
        "how_to_fix": "Replace @pandas_udf(returnType, PandasUDFType.SCALAR) with @udf and add pandas Series type hints to the function signature.",
    },
}

# SNOW-3319141: Delta Lake patterns
DELTA_LAKE_PATTERNS = {
    "DeltaTable.forPath": {
        "risk": 1.0,
        "reason": "DeltaTable API is not available in SCOS. Use session.table() or spark.table() for reading Snowflake/Iceberg tables.",
        "category": "Delta Lake",
        "how_to_fix": "Replace DeltaTable.forPath(spark, path) with spark.table(table_name). Ensure the table exists as an Iceberg table in Snowflake.",
    },
    "DeltaTable.forName": {
        "risk": 1.0,
        "reason": "DeltaTable API is not available in SCOS. Use session.table() or spark.table().",
        "category": "Delta Lake",
        "how_to_fix": "Replace DeltaTable.forName(spark, name) with spark.table(name).",
    },
    "delta.tables": {
        "risk": 1.0,
        "reason": "delta.tables import is not available in SCOS. Use Snowflake native table operations.",
        "category": "Delta Lake",
        "how_to_fix": "Remove delta.tables import. Use spark.table() for reads and df.write.saveAsTable() for writes.",
    },
}

# SNOW-3319141: Delta SQL patterns (OPTIMIZE, VACUUM, MERGE INTO on delta paths)
DELTA_SQL_KEYWORDS = ["OPTIMIZE", "VACUUM", "ZORDER"]

# RDD patterns - these indicate unsupported RDD usage
RDD_PATTERNS = [
    # SparkContext access
    ".sparkContext",
    ".rdd",
    # RDD imports
    "from pyspark import RDD",
    "from pyspark.rdd import",
    # SparkContext-specific methods - these methods only exist on SparkContext, so any .methodName( is RDD usage
    ".parallelize(",
    ".textFile(",
    ".wholeTextFiles(",
    ".binaryFiles(",
    ".binaryRecords(",
    ".hadoopFile(",
    ".hadoopRDD(",
    ".newAPIHadoopFile(",
    ".newAPIHadoopRDD(",
    ".sequenceFile(",
    ".objectFile(",
    ".pickleFile(",
    ".emptyRDD(",
]

# RDD methods - operations on RDD objects
RDD_METHODS = {
    "map",
    "flatMap",
    "filter",
    "reduce",
    "reduceByKey",
    "reduceByKeyLocally",
    "groupByKey",
    "sortByKey",
    "sortBy",
    "join",
    "leftOuterJoin",
    "rightOuterJoin",
    "fullOuterJoin",
    "cogroup",
    "cartesian",
    "pipe",
    "coalesce",
    "repartition",
    "foreach",
    "foreachPartition",
    "collect",
    "count",
    "first",
    "take",
    "takeSample",
    "takeOrdered",
    "saveAsTextFile",
    "saveAsSequenceFile",
    "saveAsObjectFile",
    "countByKey",
    "countByValue",
    "aggregate",
    "fold",
    "glom",
    "mapPartitions",
    "mapPartitionsWithIndex",
    "zip",
    "zipWithIndex",
    "zipWithUniqueId",
    "keyBy",
    "keys",
    "values",
    "lookup",
    "top",
    "max",
    "min",
    "sum",
    "mean",
    "variance",
    "stdev",
    "sampleStdev",
    "sampleVariance",
    "histogram",
    "randomSplit",
    "union",
    "intersection",
    "subtract",
    "distinct",
    "cache",
    "persist",
    "unpersist",
    "checkpoint",
    "isCheckpointed",
    "getCheckpointFile",
    "toLocalIterator",
    "isEmpty",
    "getNumPartitions",
    "mapValues",
    "flatMapValues",
    "groupWith",
    "combineByKey",
    "aggregateByKey",
    "foldByKey",
    "sampleByKey",
}

# UDF serialization patterns - these indicate potential cloudpickle serialization issues
# when running on Snowflake's server-side Python worker
UDF_SERIALIZATION_PATTERNS = [
    ".applyInPandas(",
    ".mapInPandas(",
    "@udf(",
    "@udf\n",
    "@pandas_udf(",
    "@pandas_udf\n",
    "udf(",
]

# Checkpoint patterns - not supported in SCOS, replace with cache()
CHECKPOINT_PATTERNS = [
    ".checkpoint(",
    ".checkpoint()",
    ".localCheckpoint(",
    ".localCheckpoint()",
]

# Map column subscript with Column key - not supported in Spark Connect
# map_col[col("key")] fails; use element_at(map_col, col("key")) instead
MAP_SUBSCRIPT_PATTERN = r'\]\s*\[\s*col\s*\('

# =============================================================================
# UNSUPPORTED SPARK APIs (from Snowflake documentation)
# https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-compatibility
# =============================================================================

# APIs that are completely unsupported or no-op in SCOS (risk on 0-1 scale)
UNSUPPORTED_APIS = {
    # DataFrame methods that are no-ops
    "hint": {
        "risk": 0.2,  # Low risk - just ignored
        "reason": "DataFrame.hint() is ignored in SCOS - Snowflake optimizer handles execution",
        "category": "No-Op API",
    },
    "repartition": {
        "risk": 0.2,
        "reason": "DataFrame.repartition() is a no-op in SCOS - Snowflake manages partitioning",
        "category": "No-Op API",
    },
    "coalesce": {
        "risk": 0.2,
        "reason": "DataFrame.coalesce() is a no-op in SCOS - Snowflake manages partitioning",
        "category": "No-Op API",
    },
}

# Modules/imports that indicate unsupported features (risk on 0-1 scale)
UNSUPPORTED_IMPORTS = {
    "pyspark.ml": {
        "risk": 1.0,
        "reason": "pyspark.ml (MLlib) is not supported in SCOS",
        "category": "Unsupported Module",
        "how_to_fix": "Use Snowflake ML or Snowpark ML instead",
    },
    "pyspark.streaming": {
        "risk": 1.0,
        "reason": "pyspark.streaming is not supported in SCOS",
        "category": "Unsupported Module",
        "how_to_fix": "Use Snowflake Streams and Tasks for streaming workloads",
    },
    "pyspark.mllib": {
        "risk": 1.0,
        "reason": "pyspark.mllib is not supported in SCOS",
        "category": "Unsupported Module",
        "how_to_fix": "Use Snowflake ML or Snowpark ML instead",
    },
}

# =============================================================================
# SNOWFLAKE CONNECTOR PUSHDOWN (recommended improvement, not a required fix)
# =============================================================================

SNOWFLAKE_CONNECTOR_PATTERN = {
    "risk": 0.2,
    "reason": (
        "Snowflake Connector for Spark (.format('snowflake')) is supported in SCOS but "
        "SnowflakeSession.sql() provides a better experience -- simpler code, no connector "
        "config boilerplate, and direct use of the Snowpark Connect session."
    ),
    "category": "Recommended Improvement",
    "how_to_fix": (
        "Consider replacing the .read.format('snowflake')...load() chain with "
        "SnowflakeSession.sql() for a cleaner integration. "
        "See the Snowflake Connector Pushdown rule for the complete pattern."
    ),
}

# =============================================================================
# DATA SOURCE LIMITATIONS
# =============================================================================

# File formats that are completely unsupported (risk on 0-1 scale)
UNSUPPORTED_FORMATS = {
    "avro": {
        "risk": 1.0,
        "reason": "Avro format is not supported in SCOS",
        "category": "Unsupported Format",
        "how_to_fix": "Convert data to Parquet, CSV, or JSON format",
    },
    "orc": {
        "risk": 1.0,
        "reason": "ORC format is not supported in SCOS",
        "category": "Unsupported Format",
        "how_to_fix": "Convert data to Parquet, CSV, or JSON format",
    },
    "delta": {
        "risk": 1.0,
        "reason": "Delta format is not supported in SCOS",
        "category": "Unsupported Format",
        "how_to_fix": "Convert data to Parquet, CSV, or JSON format",
    },
    "binaryFile": {
        "risk": 1.0,
        "reason": "Binary format is not supported in SCOS",
        "category": "Unsupported Format",
        "how_to_fix": "Convert data to Parquet, CSV, or JSON format",
    },
}

# File formats with partial support and their limitations
FORMAT_LIMITATIONS = {
    "csv": {
        "unsupported_modes": ["ignore"],
        "unsupported_options": [
            "quote",
            "quoteAll",
            "escapeQuotes",
            "comment",
            "preferDate",
            "enforceSchema",
            "ignoreLeadingWhiteSpace",
            "ignoreTrailingWhiteSpace",
            "nanValue",
            "positiveInf",
            "negativeInf",
            "timestampNTZFormat",
            "enableDateTimeParsingFallback",
            "maxColumns",
            "maxCharsPerColumn",
            "mode",
            "columnNameOfCorruptRecord",
            "charToEscapeQuoteEscaping",
            "samplingRatio",
            "emptyValue",
            "locale",
            "lineSep",
            "unescapedQuoteHandling",
        ],
    },
    "json": {
        "unsupported_modes": ["ignore"],
        "unsupported_options": [
            "timeZone",
            "primitiveSCOSString",
            "prefersDecimal",
            "allowComments",
            "allowUnquotedFieldNames",
            "allowSingleQuotes",
            "allowNumericLeadingZeros",
            "allowBackslashEscapingAnyCharacter",
            "mode",
            "columnNameOfCorruptRecord",
            "timestampNTZFormat",
            "enableDateTimeParsingFallback",
            "allowUnquotedControlChars",
            "encoding",
            "lineSep",
            "samplingRatio",
            "dropFieldIfAllNull",
            "locale",
            "allowNonNumericNumbers",
            "compression",
            "ignoreNullFields",
        ],
    },
    "parquet": {
        "unsupported_modes": ["ignore"],
        "unsupported_options": [
            "datetimeRebaseMode",
            "int96RebaseMode",
            "mergeSchema",
        ],
    },
    "text": {
        "unsupported_modes": ["ignore"],
        "unsupported_options": [],
    },
    "xml": {
        "unsupported_modes": ["ignore"],
        "unsupported_options": [
            "arrayElementName",
            "dateFormat",
            "declaration",
            "inferSchema",
            "locale",
            "modifiedBefore",
            "recursiveFileLookup",
            "rootTag",
            "samplingRatio",
            "timeZone",
            "timestampFormat",
            "timestampNTZFormat",
            "validateName",
            "wildcardColName",
        ],
    },
}

# Unsupported data types (risk on 0-1 scale)
UNSUPPORTED_DATATYPES = {}

# =============================================================================
# SUPPORTED SPARK CONFIGS IN SCOS
# Configs NOT in this set are no-ops (silently ignored by SCOS)
# Based on src/snowflake/snowpark_connect/config.py
# =============================================================================

# Configs that have actual effects in SCOS (Snowflake session, Snowpark behavior, etc.)
SUPPORTED_CONFIGS = {
    # Configs with Snowflake session effects (set_snowflake_parameters)
    "spark.sql.session.timeZone",
    "spark.sql.globalTempDatabase",
    "spark.sql.parquet.outputTimestampType",
    # Configs with Snowpark session effects (snowpark_config_mapping)
    "spark.app.name",
    "snowpark.connect.udf.imports",
    "snowpark.connect.udf.python.imports",
    "snowpark.connect.udf.java.imports",
    # Configs read by SCOS logic (default_global_config)
    "spark.driver.host",
    "spark.sql.pyspark.inferNestedDictAsStruct.enabled",
    "spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled",
    "spark.sql.repl.eagerEval.enabled",
    "spark.sql.repl.eagerEval.maxNumRows",
    "spark.sql.repl.eagerEval.truncate",
    "spark.sql.session.localRelationCacheThreshold",
    "spark.sql.timestampType",
    "spark.sql.crossJoin.enabled",
    "spark.sql.caseSensitive",
    "spark.sql.mapKeyDedupPolicy",
    "spark.sql.ansi.enabled",
    "spark.sql.legacy.allowHashOnMapType",
    "spark.sql.sources.default",
    "spark.Catalog.databaseFilterInformationSchema",
    "spark.sql.parser.quotedRegexColumnNames",
    "spark.sql.execution.arrow.maxRecordsPerBatch",
    "spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue",
    # Session config whitelist (AWS/Azure credentials)
    "spark.hadoop.fs.s3a.access.key",
    "spark.hadoop.fs.s3a.secret.key",
    "spark.hadoop.fs.s3a.session.token",
    "spark.hadoop.fs.s3a.server-side-encryption.key",
    "spark.hadoop.fs.s3a.assumed.role.arn",
    "spark.sql.execution.pythonUDTF.arrow.enabled",
    "spark.sql.tvf.allowMultipleTableArguments.enabled",
    "spark.sql.parquet.enable.summary-metadata",
    "spark.jars",
    "mapreduce.fileoutputcommitter.marksuccessfuljobs",
    "parquet.enable.summary-metadata",
    # Snowpark Connect specific configs (these have effects in SCOS)
    # Note: All snowpark.connect.* configs are also matched by prefix, listed here for documentation
    "snowpark.connect.sql.passthrough",  # Enables SQL passthrough mode
    "snowpark.connect.cte.optimization_enabled",  # Enables CTE optimization
    "snowpark.connect.iceberg.external_volume",  # Iceberg external volume
    "snowpark.connect.sql.identifiers.auto-uppercase",  # Identifier case handling
    "snowpark.connect.sql.partition.external_table_location",  # External table location
    "snowpark.connect.udtf.compatibility_mode",  # UDTF compatibility
    "snowpark.connect.views.duplicate_column_names_handling_mode",  # View column handling
    "snowpark.connect.temporary.views.create_in_snowflake",  # Temp view creation
    "snowpark.connect.enable_snowflake_extension_behavior",  # Snowflake extensions
    "snowpark.connect.describe_cache_ttl_seconds",  # Describe cache TTL
    "snowpark.connect.structured_types.fix",  # Structured types fix
    "snowpark.connect.scala.version",  # Scala version for Java UDFs (config exists in SCOS)
    "snowpark.connect.integralTypesEmulation",  # Integral types emulation
    "snowpark.connect.localRelation.optimizeSmallData",  # Local relation optimization
    "snowpark.connect.parquet.useVectorizedScanner",  # Parquet vectorized scanner
    "snowpark.connect.parquet.useLogicalType",  # Parquet logical types
    "snowpark.connect.handleIntegralOverflow",  # Integral overflow handling
    "snowpark.connect.version",  # SCOS version (read-only)
    # Snowflake specific configs
    "snowflake.repartition.for.writes",  # Repartition for writes
}


def is_supported_config(config_key: str) -> bool:
    """Check if a Spark config key is supported by SCOS."""
    # Check exact match
    if config_key in SUPPORTED_CONFIGS:
        return True
    return False


def check_config_no_ops(code: str) -> list[dict]:
    """
    Check for Spark config settings that are no-ops in SCOS.

    Detects patterns like:
    - spark.conf.set("key", "value")
    - .config("key", "value") in builder chains
    - SparkConf().set("key", "value")

    Returns:
        List of issues found with no-op configs
    """
    issues = []

    # Pattern 1: spark.conf.set("key", "value") or spark.conf.set('key', 'value')
    conf_set_pattern = r'\.conf\.set\s*\(\s*["\']([^"\']+)["\']\s*,'

    # Pattern 2: .config("key", "value") in builder chains
    config_pattern = r'\.config\s*\(\s*["\']([^"\']+)["\']\s*,'

    # Pattern 3: SparkConf().set("key", "value")
    sparkconf_set_pattern = r'SparkConf\s*\(\s*\).*\.set\s*\(\s*["\']([^"\']+)["\']\s*,'

    all_patterns = [
        (conf_set_pattern, "spark.conf.set()"),
        (config_pattern, ".config()"),
        (sparkconf_set_pattern, "SparkConf().set()"),
    ]

    found_configs = set()  # Track found configs to avoid duplicates

    for pattern, pattern_name in all_patterns:
        for match in re.finditer(pattern, code):
            config_key = match.group(1)

            # Skip if already reported
            if config_key in found_configs:
                continue

            # Check if this config is supported
            if not is_supported_config(config_key):
                found_configs.add(config_key)
                issues.append(
                    {
                        "api": config_key,
                        "risk": 0.2,  # Low risk - config is just ignored
                        "reason": f"Spark config '{config_key}' is a no-op in SCOS - this setting has no effect",
                        "category": "No-Op Config",
                        "how_to_fix": f"No action needed — config '{config_key}' is silently ignored in SCOS and does not cause errors",
                        "pattern": pattern_name,
                    }
                )

    return issues


@dataclass
class APIInfo:
    """API compatibility information."""

    name: str
    api_type: str
    compatibility: str
    is_supported: bool
    score: float | None  # 0-1 scale

    @classmethod
    def from_csv_row(cls, row: dict) -> "APIInfo":
        compat = row.get("COMPATIBILITY", "UNKNOWN").strip().upper()
        # Normalize compatibility values
        if compat.startswith("SHEET_"):
            compat = compat.replace("SHEET_", "")
        if compat not in COMPAT_SCORES:
            compat = "UNKNOWN"

        return cls(
            name=row.get("API", ""),
            api_type=row.get("TYPE", ""),
            compatibility=compat,
            is_supported=row.get("IS_SUPPORTED", "").lower() == "true",
            score=COMPAT_SCORES.get(compat),
        )


def load_api_compatibility(csv_path: Path) -> tuple[dict[str, APIInfo], set[str]]:
    """
    Load API compatibility data from CSV.

    Returns:
        - api_map: dict mapping API names to APIInfo
        - all_methods: set of all method/function names for detection
    """
    api_map = {}
    all_methods = set()

    if not csv_path.exists():
        logger.warning(f"Warning: API compatibility CSV not found at {csv_path}")
        return api_map, all_methods

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            info = APIInfo.from_csv_row(row)
            if info.name:
                # Store by full path
                api_map[info.name] = info

                # Also store by short name (last part) for easier lookup
                # Prefer BETTER compatibility when there are conflicts
                short_name = info.name.split(".")[-1]
                if short_name not in api_map:
                    api_map[short_name] = info
                elif info.score is not None:
                    existing = api_map[short_name]
                    # Prefer higher compatibility score (D0=100 > D1=80 > D2=50 > NONE=0)
                    if existing.score is None or info.score > existing.score:
                        api_map[short_name] = info

                # Add to methods set (for function/method types)
                if info.api_type in ("function", "method"):
                    all_methods.add(short_name)

    return api_map, all_methods


def has_rdd_usage(code: str) -> tuple[bool, str | None]:
    """
    Check if code contains RDD patterns.

    Returns:
        - (True, reason) if RDD usage detected
        - (False, None) otherwise
    """
    code_lower = code.lower()

    # Check for RDD access patterns
    for pattern in RDD_PATTERNS:
        if pattern.lower() in code_lower:
            return True, f"Uses '{pattern}' which is not supported in SCOS"

    # Check for RDD type annotations (e.g., -> RDD, : RDD)
    if re.search(r":\s*RDD\b|->.*\bRDD\b", code):
        return True, "Uses RDD type annotation which indicates RDD usage"

    # Check if it looks like RDD method chain (e.g., .map(...).filter(...))
    # Only flag if we see RDD-specific patterns
    if (
        ".rdd" in code_lower
        or "sparkcontext" in code_lower
        or re.search(r"\bsc\.", code_lower)
    ):
        for method in RDD_METHODS:
            if f".{method.lower()}(" in code_lower:
                return True, f"RDD operation '.{method}()' is not supported in SCOS"

    return False, None


def check_unsupported_apis(code: str) -> list[dict]:
    """
    Check for unsupported Spark APIs in code.

    Returns:
        List of issues found, each with risk, reason, category, how_to_fix
    """
    issues = []

    # Check for unsupported imports
    for module, info in UNSUPPORTED_IMPORTS.items():
        # Check for import statements
        if f"import {module}" in code or f"from {module}" in code:
            issues.append(
                {
                    "api": module,
                    "risk": info["risk"],
                    "reason": info["reason"],
                    "category": info["category"],
                    "how_to_fix": info.get("how_to_fix"),
                }
            )

    # Check for unsupported/no-op DataFrame methods
    for method, info in UNSUPPORTED_APIS.items():
        if f".{method}(" in code:
            issues.append(
                {
                    "api": method,
                    "risk": info["risk"],
                    "reason": info["reason"],
                    "category": info["category"],
                    "how_to_fix": info.get("how_to_fix"),
                }
            )

    # Check for unsupported data types in schema definitions
    for dtype, info in UNSUPPORTED_DATATYPES.items():
        if dtype in code:
            issues.append(
                {
                    "api": dtype,
                    "risk": info["risk"],
                    "reason": info["reason"],
                    "category": info["category"],
                    "how_to_fix": info.get("how_to_fix"),
                }
            )

    # Check for checkpoint usage
    for pattern in CHECKPOINT_PATTERNS:
        if pattern in code:
            issues.append(
                {
                    "api": "checkpoint",
                    "risk": 0.9,
                    "reason": "DataFrame.checkpoint() is not supported in SCOS — replace with cache()",
                    "category": "Unsupported API",
                    "how_to_fix": "Replace .checkpoint() and .localCheckpoint() with .cache()",
                }
            )
            break  # Only report once

    # Check for UDF serialization patterns
    for pattern in UDF_SERIALIZATION_PATTERNS:
        if pattern in code:
            issues.append(
                {
                    "api": "UDF serialization",
                    "risk": 0.6,
                    "reason": "UDF may have serialization issues in SCOS — cloudpickle may fail on helper functions or module-level references",
                    "category": "UDF Serialization",
                    "how_to_fix": "Make UDF self-contained (Tier 2), use stage imports (Tier 1), or apply __module__ patching (Tier 3). See references/python/udf-dependencies.md",
                }
            )
            break  # Only report once

    # Check for map column subscript with Column key
    if re.search(MAP_SUBSCRIPT_PATTERN, code):
        issues.append(
            {
                "api": "Map column subscript",
                "risk": 0.9,
                "reason": "Map column subscript with Column key (map_col[col('key')]) is not supported in Spark Connect — use element_at() instead",
                "category": "Unsupported API",
                "how_to_fix": "Replace map_col[col('key')] with element_at(map_col, col('key'))",
            }
        )

    return issues


def check_data_source_issues(code: str) -> list[dict]:
    """
    Check for data source compatibility issues.

    Returns:
        List of issues found with format/option problems
    """
    issues = []
    code_lower = code.lower()

    # Detect Snowflake Connector pushdown pattern (supported but SnowflakeSession is better UX)
    sf_connector_patterns = ['.format("snowflake")', ".format('snowflake')"]
    for pattern in sf_connector_patterns:
        if pattern.lower() in code_lower:
            issues.append(
                {
                    "api": "Snowflake Connector pushdown",
                    "risk": SNOWFLAKE_CONNECTOR_PATTERN["risk"],
                    "reason": SNOWFLAKE_CONNECTOR_PATTERN["reason"],
                    "category": SNOWFLAKE_CONNECTOR_PATTERN["category"],
                    "how_to_fix": SNOWFLAKE_CONNECTOR_PATTERN["how_to_fix"],
                }
            )
            break

    # Check for unsupported file formats
    # Pattern: .format("avro") or .load("file.avro")
    for fmt, info in UNSUPPORTED_FORMATS.items():
        patterns = [
            f'.format("{fmt}")',
            f".format('{fmt}')",
            f".{fmt}(",  # e.g., .avro(), .orc()
            f'.load("{fmt}',
            f".load('{fmt}",
        ]
        for pattern in patterns:
            if pattern.lower() in code_lower:
                issues.append(
                    {
                        "format": fmt,
                        "risk": info["risk"],
                        "reason": info["reason"],
                        "category": info["category"],
                        "how_to_fix": info.get("how_to_fix"),
                    }
                )
                break  # Only report once per format

    # Check file extensions in paths
    for fmt in UNSUPPORTED_FORMATS:
        if f".{fmt}" in code_lower and ("load(" in code_lower or "read" in code_lower):
            info = UNSUPPORTED_FORMATS[fmt]
            # Avoid duplicate if already caught above
            if not any(i.get("format") == fmt for i in issues):
                issues.append(
                    {
                        "format": fmt,
                        "risk": info["risk"],
                        "reason": info["reason"],
                        "category": info["category"],
                        "how_to_fix": info.get("how_to_fix"),
                    }
                )

    # Check for unsupported save modes
    for fmt, limits in FORMAT_LIMITATIONS.items():
        # Only check if this format is being used
        if (
            f'.format("{fmt}")' in code_lower
            or f".format('{fmt}')" in code_lower
            or f".{fmt}(" in code_lower
        ):
            for mode in limits.get("unsupported_modes", []):
                mode_patterns = [
                    f'.mode("{mode}")',
                    f".mode('{mode}')",
                    f'.mode("{mode.lower()}")',
                    f".mode('{mode.lower()}')",
                ]
                for pattern in mode_patterns:
                    if pattern.lower() in code_lower:
                        issues.append(
                            {
                                "format": fmt,
                                "risk": 0.7,
                                "reason": f"Save mode '{mode}' is not supported for {fmt.upper()} in SCOS",
                                "category": "Unsupported Save Mode",
                                "how_to_fix": f"Use 'overwrite' or 'errorifexists' mode instead of '{mode}'",
                            }
                        )
                        break

    # Check for unsupported options
    for fmt, limits in FORMAT_LIMITATIONS.items():
        if (
            f'.format("{fmt}")' in code_lower
            or f".format('{fmt}')" in code_lower
            or f".{fmt}(" in code_lower
        ):
            for opt in limits.get("unsupported_options", []):
                opt_patterns = [
                    f'.option("{opt}"',
                    f".option('{opt}'",
                ]
                for pattern in opt_patterns:
                    if pattern.lower() in code_lower:
                        issues.append(
                            {
                                "format": fmt,
                                "risk": 0.5,
                                "reason": f"Option '{opt}' is not supported for {fmt.upper()} in SCOS",
                                "category": "Unsupported Option",
                                "how_to_fix": f"Remove or work around the '{opt}' option",
                            }
                        )
                        break

    # Check for file read operations - performance optimization
    # Reading from external files (cloud storage, local paths) may be slower than
    # reading from Snowflake internal stages. Add advisory for any file read.
    file_read_patterns = [
        (r"\.read\.csv\s*\(", "csv"),
        (r"\.read\.json\s*\(", "json"),
        (r"\.read\.parquet\s*\(", "parquet"),
        (r"\.read\.text\s*\(", "text"),
        (r"\.read\.orc\s*\(", "orc"),
        (r"\.load\s*\(", "load"),
    ]

    for pattern, read_type in file_read_patterns:
        if re.search(pattern, code, re.IGNORECASE):
            issues.append(
                {
                    "api": f"file read ({read_type})",
                    "risk": 0.2,
                    "reason": (
                        "Reading from external files (S3, Azure, GCS, local paths) may be slower than "
                        "reading from Snowflake internal stage. For better performance, "
                        "consider uploading files to a Snowflake stage first."
                    ),
                    "category": "Performance Optimization",
                    "how_to_fix": (
                        "Upload files to a Snowflake stage using session.file.put() for faster processing. "
                        "Example: session.file.put('file:///local/path/data.csv', '@MY_STAGE/data/', auto_compress=False). "
                    ),
                }
            )
            break

    return issues


def check_udf_serialization_issues(code: str) -> list[dict]:
    """
    Check for applyInPandas/mapInPandas patterns that may cause
    cloudpickle serialization issues on Snowflake's server-side worker.

    Detects:
    - applyInPandas/mapInPandas usage (potential serialization risk)
    - UDF functions that call other functions defined in the same module

    Returns:
        List of issues found with UDF serialization risks
    """
    issues = []

    for pattern in UDF_SERIALIZATION_PATTERNS:
        if pattern in code:
            api_name = pattern.strip(".(")
            issues.append(
                {
                    "api": api_name,
                    "risk": 0.5,
                    "reason": (
                        f"{api_name} UDFs are serialized with cloudpickle for server-side execution. "
                        "If the UDF calls helper functions defined in the workload module, "
                        "cloudpickle will try to import the workload module on the server, "
                        "causing ModuleNotFoundError. Also, any third-party packages imported "
                        "by the UDF must be available in Snowflake's Anaconda channel."
                    ),
                    "category": "UDF Serialization",
                    "how_to_fix": (
                        "See references/udf-dependencies.md for the tiered fix approach: "
                        "(1) Use snowpark.connect.udf.packages / snowpark.connect.udf.python.imports "
                        "for external dependencies. "
                        "(2) Keep UDF logic self-contained (inline). "
                        "(3) For complex UDFs with many helpers, use factory functions + "
                        "__module__ = '__main__' patching on the UDF and all helpers in its call chain."
                    ),
                }
            )
            break  # Only report once

    return issues


# SNOW-3256946, SNOW-3256947, SNOW-3256949, SNOW-3256948, SNOW-3256950:
# Memory anti-patterns, known issues, case sensitivity, UDF config, performance
def check_memory_and_known_issues(code: str) -> list[dict]:
    """
    Check for memory anti-patterns, known SCOS issues, case sensitivity concerns,
    UDF configuration needs, and performance anti-patterns.

    Detects:
    - .count() / .collect() / .cache() / .toPandas() on large DataFrames (SNOW-3256947)
    - saveAsTable for transient tables (SNOW-3256949)
    - QUALIFY clause in spark.sql() (SNOW-3256949)
    - Cross join anti-patterns (SNOW-3256950)
    - Case sensitivity / INSERT SELECT * patterns (SNOW-3256946)
    - UDF package dependency needs (SNOW-3256948)
    """
    issues = []

    # SNOW-3256947: Memory anti-pattern detection
    # .count() on DataFrames (can hang on large data)
    if re.search(r"\.count\s*\(\s*\)", code):
        issues.append({
            "api": ".count()",
            "risk": 0.4,
            "reason": (
                "DataFrame.count() can hang on large datasets in SCOS. "
                "Consider using SQL COUNT via SnowflakeSession for safer execution."
            ),
            "category": "Memory Anti-Pattern",
            "how_to_fix": (
                "Replace with SnowflakeSession SQL: "
                "df.createOrReplaceTempView('_tmp'); "
                "snowflake_session.sql('SELECT COUNT(*) FROM _tmp').collect()[0][0]"
            ),
        })

    # .collect() on DataFrames (OOM risk)
    if re.search(r"\.collect\s*\(\s*\)", code):
        # Only flag if not preceded by .sql(...).collect() which is a small result pattern
        if not re.search(r"\.sql\s*\([^)]+\)\s*\.collect\s*\(\s*\)", code):
            issues.append({
                "api": ".collect()",
                "risk": 0.5,
                "reason": (
                    "DataFrame.collect() transfers all data to the driver and can cause OOM "
                    "on large datasets in SCOS. Only safe for small result sets."
                ),
                "category": "Memory Anti-Pattern",
                "how_to_fix": (
                    "If the result set is small (e.g., aggregation), this is safe. "
                    "For large datasets, use SnowflakeSession.sql() with LIMIT, "
                    "or process data in Snowflake directly."
                ),
            })

    # .cache() on DataFrames (temp view lifecycle differs)
    if re.search(r"\.cache\s*\(\s*\)", code):
        issues.append({
            "api": ".cache()",
            "risk": 0.4,
            "reason": (
                "DataFrame.cache() in SCOS creates temp view references that may become "
                "invalid if the source is dropped. Unlike native Spark, cached data does "
                "not survive source view drops."
            ),
            "category": "Memory Anti-Pattern",
            "how_to_fix": (
                "Replace with checkpoint-to-temp-table via SnowflakeSession CTAS: "
                "df.createOrReplaceTempView('_tmp_src'); "
                "snowflake_session.sql('CREATE OR REPLACE TEMPORARY TABLE _cached AS "
                "SELECT * FROM _tmp_src').collect(); df_cached = spark.table('_cached')"
            ),
        })

    # .toPandas() on DataFrames (driver OOM risk)
    if re.search(r"\.toPandas\s*\(\s*\)", code):
        issues.append({
            "api": ".toPandas()",
            "risk": 0.5,
            "reason": (
                "DataFrame.toPandas() transfers all data to driver memory and can cause OOM. "
                "Consider adding .limit(N) or processing in Snowflake."
            ),
            "category": "Memory Anti-Pattern",
            "how_to_fix": (
                "Add .limit(N) before .toPandas() if only a sample is needed, "
                "or process data in Snowflake using SnowflakeSession.sql(), "
                "or export to stage with df.write.csv() and read with pandas."
            ),
        })

    # SNOW-3256949: Known issues detection
    # QUALIFY clause in spark.sql() — not supported via Spark Connect
    if re.search(r"spark\.sql\s*\(.*QUALIFY\b", code, re.IGNORECASE | re.DOTALL):
        issues.append({
            "api": "QUALIFY clause",
            "risk": 0.7,
            "reason": (
                "QUALIFY clause is Snowflake-specific and not supported in standard "
                "Spark SQL via Spark Connect. Route through SnowflakeSession.sql() instead."
            ),
            "category": "Known SCOS Issue",
            "how_to_fix": (
                "Replace spark.sql('...QUALIFY...') with "
                "snowflake_session.sql('...QUALIFY...')"
            ),
        })

    # SNOW-3256950: Performance anti-patterns
    # Cross join detection
    if re.search(r"\.crossJoin\s*\(", code):
        issues.append({
            "api": "crossJoin()",
            "risk": 0.6,
            "reason": (
                "Cross joins can cause data explosion. If followed by a filter on matching keys, "
                "rewrite as a keyed inner join for better performance."
            ),
            "category": "Performance Anti-Pattern",
            "how_to_fix": (
                "Rewrite crossJoin().filter(df1['id'] == df2['id']) as "
                "df1.join(df2, df1['id'] == df2['id'], 'inner')"
            ),
        })

    # SNOW-3256946: Case sensitivity — INSERT ... SELECT * pattern
    if re.search(
        r"""spark\.sql\s*\(\s*["']INSERT\s+INTO\s+\S+\s+SELECT\s+\*""",
        code,
        re.IGNORECASE,
    ):
        issues.append({
            "api": "INSERT INTO ... SELECT *",
            "risk": 0.5,
            "reason": (
                "INSERT INTO ... SELECT * relies on column ordering which may differ "
                "between Spark and Snowflake. Use explicit column lists to avoid mismatch."
            ),
            "category": "Case Sensitivity",
            "how_to_fix": (
                "Replace SELECT * with explicit column list: "
                "INSERT INTO tbl (col1, col2) SELECT col1, col2 FROM src"
            ),
        })

    # SNOW-3256948: UDF package configuration detection
    # Detect UDFs that import third-party packages needing snowpark.connect.udf.packages
    udf_import_patterns = [
        (r"@udf\b", "UDF detected"),
        (r"@pandas_udf\b", "Pandas UDF detected"),
        (r"\.udf\.register\s*\(", "UDF registration detected"),
    ]
    for pattern, desc in udf_import_patterns:
        if re.search(pattern, code):
            # Check if the same block imports third-party packages
            third_party_imports = re.findall(
                r"import\s+(numpy|pandas|scipy|sklearn|scikit|requests|boto3|cryptography)",
                code,
            )
            if third_party_imports:
                packages = ", ".join(sorted(set(third_party_imports)))
                issues.append({
                    "api": f"UDF with imports: {packages}",
                    "risk": 0.5,
                    "reason": (
                        f"{desc} with third-party imports ({packages}). "
                        "These packages must be configured via snowpark.connect.udf.packages "
                        "for Snowflake server-side execution."
                    ),
                    "category": "UDF Configuration",
                    "how_to_fix": (
                        f'spark.conf.set("snowpark.connect.udf.packages", "{packages}")'
                    ),
                })
            break

    return issues


# SNOW-3347695: Check for per-property SparkContext access patterns
def check_spark_context_properties(code: str) -> list[dict]:
    """
    Check for sparkContext property access patterns and return per-property issues
    with individual risk scores and replacement suggestions.
    """
    issues = []
    found_properties = set()

    for prop, info in SPARK_CONTEXT_PROPERTIES.items():
        # Match patterns like: sparkContext.master, sc.master, spark.sparkContext.master
        patterns = [
            f".sparkContext.{prop}",
            f"sparkContext.{prop}",
            f"sc.{prop}(",  # method call form
        ]
        # For non-method properties, also match without parens
        if prop not in ("parallelize", "textFile", "broadcast", "accumulator", "getConf"):
            patterns.append(f"sc.{prop}")

        for pattern in patterns:
            if pattern in code and prop not in found_properties:
                found_properties.add(prop)
                issue = {
                    "api": f"sparkContext.{prop}",
                    "risk": info["risk"],
                    "reason": info["reason"],
                    "category": info["category"],
                }
                if info.get("replacement"):
                    issue["how_to_fix"] = f"Replace with: {info['replacement']}"
                else:
                    issue["how_to_fix"] = info["reason"]
                issues.append(issue)
                break

    return issues


# SNOW-3347699: Check for Hadoop filesystem access patterns
def check_hadoop_patterns(code: str) -> list[dict]:
    """
    Check for Hadoop FileSystem API calls, DBFS paths, and Hadoop credential configuration.
    """
    issues = []

    # Check for Hadoop API patterns
    for pattern_name, info in HADOOP_PATTERNS.items():
        if pattern_name in code:
            issues.append(
                {
                    "api": pattern_name,
                    "risk": info["risk"],
                    "reason": info["reason"],
                    "category": info["category"],
                    "how_to_fix": info.get("how_to_fix"),
                }
            )

    # Check for broader Hadoop patterns via regex
    hadoop_regex_patterns = [
        (r"org\.apache\.hadoop\.fs\.FileSystem", "Hadoop FileSystem API"),
        (r"org\.apache\.hadoop\.fs\.Path", "Hadoop Path API"),
        (r"org\.apache\.hadoop\.conf\.Configuration", "Hadoop Configuration API"),
        (r"sc\._jvm\.org\.apache\.hadoop", "Hadoop JVM interop via SparkContext"),
        (r"_jsc\.hadoopConfiguration", "Hadoop Configuration via _jsc"),
    ]
    for regex, name in hadoop_regex_patterns:
        if re.search(regex, code) and not any(i["api"] == name for i in issues):
            issues.append(
                {
                    "api": name,
                    "risk": 1.0,
                    "reason": f"{name} requires JVM interop not available in Spark Connect.",
                    "category": "Hadoop Filesystem",
                    "how_to_fix": "Replace with cloud-native SDK (boto3/azure-storage-blob/google-cloud-storage) or Snowflake stage operations.",
                }
            )

    # SNOW-3347699: Check for DBFS path patterns
    for dbfs_pattern in DBFS_PATH_PATTERNS:
        if dbfs_pattern in code:
            issues.append(
                {
                    "api": f"DBFS path ({dbfs_pattern})",
                    "risk": 0.8,
                    "reason": f"DBFS path '{dbfs_pattern}' is Databricks-specific and not available in SCOS. Rewrite to Snowflake internal stage + COPY INTO.",
                    "category": "Hadoop Filesystem",
                    "how_to_fix": "Replace DBFS paths with Snowflake stage references (@STAGE_NAME/path). Upload data to a Snowflake stage first.",
                }
            )
            break  # Only report DBFS once

    return issues


# SNOW-3347690: Check for USE DATABASE/SCHEMA statements
def check_use_statements(code: str) -> list[dict]:
    """
    Check for USE DATABASE/SCHEMA statements and setCurrentDatabase() calls
    that are not supported in Spark Connect mode.
    """
    issues = []

    for pattern in USE_STATEMENT_PATTERNS:
        if re.search(pattern, code, re.IGNORECASE):
            issues.append(
                {
                    "api": "USE DATABASE/SCHEMA",
                    "risk": 0.9,
                    "reason": (
                        "USE DATABASE/SCHEMA and setCurrentDatabase() are not supported in Spark Connect. "
                        "Session-level database context does not propagate over gRPC. "
                        "All table references must be fully qualified (DB.SCHEMA.TABLE)."
                    ),
                    "category": "USE Statement",
                    "how_to_fix": (
                        "Comment out USE statements. Rewrite downstream unqualified table references "
                        "to fully-qualified DB.SCHEMA.TABLE format. For dynamic SQL, use a TABLE_PREFIX variable."
                    ),
                }
            )
            break  # Only report once per code block

    return issues


# SNOW-3347693: Check for JVM-only library imports
def check_jvm_library_imports(code: str) -> list[dict]:
    """
    Check for imports of JVM-dependent libraries (Deequ, pydeequ, Great Expectations Spark, etc.)
    that require SparkContext JVM interop not available in Spark Connect.
    """
    issues = []

    for module, info in JVM_ONLY_IMPORTS.items():
        if f"import {module}" in code or f"from {module}" in code:
            issues.append(
                {
                    "api": module,
                    "risk": info["risk"],
                    "reason": info["reason"],
                    "category": info["category"],
                    "how_to_fix": info.get("how_to_fix"),
                }
            )

    # Also detect Deequ usage patterns even without explicit imports
    deequ_patterns = ["VerificationSuite", "VerificationResult", "CheckLevel", "pydeequ"]
    for pattern in deequ_patterns:
        if pattern in code and not any("pydeequ" in i.get("api", "") for i in issues):
            issues.append(
                {
                    "api": f"pydeequ ({pattern})",
                    "risk": 1.0,
                    "reason": f"{pattern} is part of pydeequ/Deequ which requires JVM interop not available in Spark Connect.",
                    "category": "JVM Library",
                    "how_to_fix": JVM_ONLY_IMPORTS["pydeequ"]["how_to_fix"],
                }
            )
            break

    return issues


# SNOW-3319134: Check for ML pipeline patterns
def check_ml_pipeline_patterns(code: str) -> list[dict]:
    """
    Check for pyspark.ml pipeline patterns (estimators, VectorAssembler, Pipeline, CrossValidator)
    and provide guided transformation to snowflake.ml equivalents.
    """
    issues = []
    found_patterns = set()

    for class_name, info in ML_PIPELINE_PATTERNS.items():
        # Check for import or instantiation patterns
        patterns = [
            f"import {class_name}",
            f"from pyspark.ml",  # will be refined below
            f"{class_name}(",
        ]
        if any(p in code for p in patterns) and class_name in code and class_name not in found_patterns:
            found_patterns.add(class_name)
            issues.append(
                {
                    "api": class_name,
                    "risk": info["risk"],
                    "reason": info["reason"],
                    "category": info["category"],
                    "how_to_fix": info.get("how_to_fix"),
                }
            )

    return issues


# SNOW-3319139: Check for UDTF/UDAF patterns
def check_udtf_udaf_patterns(code: str) -> list[dict]:
    """
    Check for PySpark UDTF and UDAF patterns that need structural transformation
    for Snowpark equivalents.
    """
    issues = []

    for pattern_name, info in UDTF_UDAF_PATTERNS.items():
        if pattern_name in code:
            issues.append(
                {
                    "api": pattern_name,
                    "risk": info["risk"],
                    "reason": info["reason"],
                    "category": info["category"],
                    "how_to_fix": info.get("how_to_fix"),
                }
            )

    return issues


# SNOW-3277715: Check for lazy view re-evaluation patterns
def check_view_reuse_patterns(code: str) -> list[dict]:
    """
    SNOW-3277715: Detect temp view reuse patterns where a DataFrame references a
    view that is later overwritten via createOrReplaceTempView, or dropped via
    spark.catalog.dropTempView while still referenced.

    In Spark Classic, the logical plan is resolved eagerly. In Spark Connect (SCOS),
    the plan is unresolved and re-resolves by name on each evaluation — causing
    silent result differences or "view not found" errors.
    """
    issues = []

    # Extract all createOrReplaceTempView calls with their view names
    create_view_pattern = r'\.createOrReplaceTempView\s*\(\s*["\']([^"\']+)["\']\s*\)'
    view_creations = re.findall(create_view_pattern, code)

    # Extract all spark.sql / spark.table references to view names
    sql_from_pattern = r'spark\.sql\s*\(\s*["\'].*?FROM\s+(\w+)'
    table_ref_pattern = r'spark\.table\s*\(\s*["\'](\w+)["\']\s*\)'
    sql_refs = re.findall(sql_from_pattern, code, re.IGNORECASE | re.DOTALL)
    table_refs = re.findall(table_ref_pattern, code)
    all_view_refs = set(sql_refs + table_refs)

    # Check for view name reuse: same name created more than once
    view_counts = Counter(view_creations)
    reused_views = {name for name, count in view_counts.items() if count > 1}

    # Check for views that are both referenced and overwritten
    overwritten_refs = all_view_refs & set(view_creations)
    # Only flag if the same view name appears in createOrReplaceTempView AND is
    # referenced, AND is created more than once (indicating overwrite)
    flagged_views = overwritten_refs & reused_views

    for view_name in flagged_views:
        issues.append({
            "api": f"createOrReplaceTempView('{view_name}') — view reuse",
            "risk": 0.8,
            "reason": (
                f"Temp view '{view_name}' is overwritten via createOrReplaceTempView "
                "while an existing DataFrame may still reference it. In Spark Connect "
                "(SCOS), the DataFrame re-resolves against the new view definition on "
                "each evaluation, producing different results than Spark Classic."
            ),
            "category": "Lazy View Re-Evaluation",
            "how_to_fix": (
                f"After the overwriting createOrReplaceTempView('{view_name}'), "
                f"re-read the DataFrame: df = spark.sql(\"SELECT * FROM {view_name}\")"
            ),
        })

    # Check for dropTempView while DataFrame still references the view
    drop_view_pattern = r'\.catalog\.dropTempView\s*\(\s*["\'](\w+)["\']\s*\)'
    dropped_views = set(re.findall(drop_view_pattern, code))
    dropped_refs = all_view_refs & dropped_views

    for view_name in dropped_refs:
        issues.append({
            "api": f"dropTempView('{view_name}') — referenced after drop",
            "risk": 0.9,
            "reason": (
                f"Temp view '{view_name}' is dropped via spark.catalog.dropTempView "
                "while an existing DataFrame still references it. In Spark Connect "
                "(SCOS), the server will raise a 'view not found' error, whereas "
                "Spark Classic continues to work with the already-resolved plan."
            ),
            "category": "Lazy View Re-Evaluation",
            "how_to_fix": (
                f"Materialize the DataFrame before dropping the view, or "
                f"do not drop '{view_name}' while it is still referenced."
            ),
        })

    return issues


# SNOW-3319141: Check for Delta Lake patterns
def check_delta_lake_patterns(code: str) -> list[dict]:
    """
    Check for Delta Lake operations (DeltaTable API, delta format reads/writes,
    OPTIMIZE/VACUUM SQL) and provide Snowflake-native equivalents.
    """
    issues = []

    # Check for DeltaTable API and delta.tables import
    for pattern_name, info in DELTA_LAKE_PATTERNS.items():
        if pattern_name in code:
            issues.append(
                {
                    "api": pattern_name,
                    "risk": info["risk"],
                    "reason": info["reason"],
                    "category": info["category"],
                    "how_to_fix": info.get("how_to_fix"),
                }
            )

    # Check for Delta SQL keywords in spark.sql() calls
    code_upper = code.upper()
    for keyword in DELTA_SQL_KEYWORDS:
        if keyword in code_upper and "spark.sql" in code.lower():
            if not any(i.get("api") == f"Delta SQL: {keyword}" for i in issues):
                if keyword == "OPTIMIZE":
                    fix = "Remove OPTIMIZE. Snowflake uses automatic micro-partitioning — no manual optimization needed."
                elif keyword == "VACUUM":
                    fix = "Remove VACUUM. Snowflake manages Time Travel retention automatically."
                else:  # ZORDER
                    fix = "Replace ZORDER BY with ALTER TABLE ... CLUSTER BY for Snowflake clustering keys."
                issues.append(
                    {
                        "api": f"Delta SQL: {keyword}",
                        "risk": 0.9,
                        "reason": f"Delta Lake {keyword} SQL is not supported in SCOS. Snowflake handles this automatically.",
                        "category": "Delta Lake",
                        "how_to_fix": fix,
                    }
                )

    return issues


def _build_assessment_text(preliminary_assessment: dict) -> str:
    """Build preliminary assessment text for LLM prompt."""
    assessment_parts = []

    scos_issues = preliminary_assessment.get("scos_issues", [])
    if scos_issues:
        assessment_parts.append("SCOS Compatibility Issues:")
        for issue in scos_issues:
            issue_name = issue.get("api") or issue.get("format", "unknown")
            assessment_parts.append(
                f"  - {issue_name}: {issue['reason']} (Risk: {issue['risk'] * 100:.0f}%)"
            )
            if issue.get("how_to_fix"):
                assessment_parts.append(f"    Fix: {issue['how_to_fix']}")

    api_risk = preliminary_assessment.get("api_risk", 0)
    if api_risk > 0:
        assessment_parts.append(f"\nAPI Compatibility Risk: {api_risk * 100:.0f}%")
        func_compat = preliminary_assessment.get("func_compatibility", [])
        for f in func_compat:
            if f.get("score", 1.0) < 1.0:
                assessment_parts.append(
                    f"  - {f['name']}: {f['compatibility']} (score: {f['score'] * 100:.0f}%)"
                )

    scos_risk = preliminary_assessment.get("scos_risk", 0)
    if scos_risk > 0:
        assessment_parts.append(f"\nSCOS Issues Risk: {scos_risk * 100:.0f}%")

    return (
        "\n".join(assessment_parts)
        if assessment_parts
        else "No rule-based issues detected."
    )


def _build_patterns_text(matching_patterns: list[dict]) -> str:
    """Build matching patterns text for LLM prompt."""
    if not matching_patterns:
        return "No similar failing test cases found above similarity threshold."

    patterns_text_parts = []
    for i, p in enumerate(matching_patterns, 1):
        patterns_text_parts.append(
            f"""TEST CASE #{i} (Cosine similarity: {p.get('score', 0.0):.1%})
Test Name: {p.get('test_name', 'N/A')}
Code/SQL:
```
{p.get('code', '')}
```
Root Cause: {p.get('root_cause', 'N/A')}
Additional Notes: {p.get('additional_notes', 'N/A')}"""
        )
    return "\n\n".join(patterns_text_parts)


def predict_compatibility_batch(
    session: Session,
    batch_items: list[dict],
    model: str = DEFAULT_LLM_MODEL,
) -> dict[str, dict]:
    """
    Predict compatibility for multiple code blocks in a single LLM call.

    Args:
        session: Snowflake session
        batch_items: List of dicts with keys:
            - block_id: Unique identifier for this block
            - input_code: The code being analyzed
            - matching_patterns: List of similar failing test cases from RAG
            - preliminary_assessment: Dict with preliminary risk info
        model: LLM model to use

    Returns:
        Dict mapping block_id -> LLM result dict
    """
    if not batch_items:
        return {}

    # Build the combined prompt for all blocks
    code_blocks_parts = []
    for item in batch_items:
        block_id = item["block_id"]
        input_code = item["input_code"]
        matching_patterns = item.get("matching_patterns", [])
        preliminary_assessment = item.get("preliminary_assessment", {})

        assessment_text = _build_assessment_text(preliminary_assessment)
        patterns_text = _build_patterns_text(matching_patterns)

        code_blocks_parts.append(
            f"""### BLOCK {block_id}

```python
{input_code}
```

**Preliminary Assessment:**
{assessment_text}

**Similar Failing Test Cases:**
{patterns_text}

---"""
        )

    code_blocks_text = "\n\n".join(code_blocks_parts)

    prompt = PROMPT_PREDICT_COMPATIBILITY_BATCH.format(
        code_blocks_text=code_blocks_text,
        num_blocks=len(batch_items),
    )

    try:
        # Use temperature=0 for deterministic output
        options = CompleteOptions(temperature=0.0)
        response = cortex_complete(model, prompt, options=options, session=session)

        # Strip markdown code block if present
        response = response.strip()
        if response.startswith("```"):
            lines = response.split("\n")
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            response = "\n".join(lines)

        results_list = json.loads(response)

        # Convert list to dict keyed by block_id
        results_dict = {}
        for result in results_list:
            block_id = result.get("block_id")
            if block_id:
                results_dict[block_id] = result

        # Assert that response contains all input batches
        input_block_ids = {item["block_id"] for item in batch_items}
        response_block_ids = set(results_dict.keys())
        missing_ids = input_block_ids - response_block_ids
        assert not missing_ids, (
            f"LLM response missing {len(missing_ids)} block(s): {missing_ids}. "
            f"Expected {len(input_block_ids)} blocks, got {len(response_block_ids)}."
        )

        return results_dict

    except json.JSONDecodeError as e:
        raise ValueError(
            f"Cortex returned invalid JSON. Response (first 500 chars): {response[:500]}...\n"
            f"JSON error: {e}"
        )
    except AssertionError:
        # Re-raise assertion errors (missing block IDs)
        raise
    except Exception as e:
        raise RuntimeError(f"Batch LLM prediction failed: {e}")


_BATCH_MAX_RETRIES = 3


def predict_compatibility_batch_with_retry(session, batch_items, max_retries=_BATCH_MAX_RETRIES):
    """Wrapper with exponential backoff for transient LLM/network failures."""
    import time as _time
    for attempt in range(max_retries):
        try:
            return predict_compatibility_batch(session, batch_items)
        except (RuntimeError, ValueError) as exc:
            if attempt < max_retries - 1:
                delay = 5 * (2 ** attempt)
                logger.warning("Batch LLM attempt %d/%d failed: %s — retrying in %ds", attempt + 1, max_retries, exc, delay)
                _time.sleep(delay)
            else:
                logger.error("Batch LLM failed after %d attempts: %s", max_retries, exc)
                raise


@dataclass
class CodeBlock:
    """A block of code extracted from a PySpark file."""

    code: str
    line_start: int
    line_end: int
    block_type: str  # "sql", "expr", "method_chain", "statement"
    functions: list[str]  # Functions/methods found in this block

    @property
    def normalized_code(self) -> str:
        """Return normalized code for RAG queries (comments removed, whitespace normalized)."""
        return normalize_code_lightweight(self.code)


class PySparkExtractor(ast.NodeVisitor):
    """Extract PySpark code blocks using AST."""

    def __init__(
        self, source_lines: list[str], pyspark_methods: set[str] | None = None
    ):
        self.source_lines = source_lines
        self.blocks: list[CodeBlock] = []
        # Use provided methods or fall back to common ones
        self.pyspark_methods = pyspark_methods or {
            "select",
            "filter",
            "where",
            "groupBy",
            "agg",
            "join",
            "orderBy",
            "sort",
            "withColumn",
            "drop",
            "distinct",
            "union",
            "intersect",
            "subtract",
            "limit",
            "sample",
            "createDataFrame",
            "read",
            "write",
            "show",
            "collect",
        }

    def get_source(self, node: ast.AST) -> str:
        """Get source code for a node."""
        try:
            return ast.get_source_segment("\n".join(self.source_lines), node) or ""
        except Exception:
            # Fallback: get lines
            start = node.lineno - 1
            end = getattr(node, "end_lineno", node.lineno)
            return "\n".join(self.source_lines[start:end])

    def extract_string_value(self, node: ast.AST) -> str | None:
        """Extract string value from a node."""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.JoinedStr):
            # f-string - try to get parts
            parts = []
            for value in node.values:
                if isinstance(value, ast.Constant):
                    parts.append(str(value.value))
                elif isinstance(value, ast.FormattedValue):
                    # Extract source of the expression inside the f-string
                    parts.append("<" + self.get_source(value.value) + ">")
            return "".join(parts) if parts else None
        return None

    def extract_functions(self, code: str) -> list[str]:
        """Extract function/method names from code."""
        functions = []
        # Pattern for function calls: word followed by (
        pattern = r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\("
        for match in re.finditer(pattern, code):
            func_name = match.group(1)
            # Skip common Python keywords and builtins
            if func_name not in [
                "if",
                "for",
                "while",
                "with",
                "def",
                "class",
                "print",
                "len",
                "str",
                "int",
                "list",
                "dict",
                "set",
                "tuple",
            ]:
                functions.append(func_name)
        return list(set(functions))

    def _has_call_nodes(self, node: ast.AST) -> bool:
        """Check if an AST node contains any function/method calls."""
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                return True
        return False

    def visit_Call(self, node: ast.Call):
        """Visit function/method calls."""
        # Check for spark.sql(...) or session.sql(...)
        if isinstance(node.func, ast.Attribute) and node.func.attr == "sql":
            if node.args:
                sql_str = self.extract_string_value(node.args[0])
                if sql_str:
                    self.blocks.append(
                        CodeBlock(
                            code=sql_str,
                            line_start=node.lineno,
                            line_end=getattr(node, "end_lineno", node.lineno),
                            block_type="sql",
                            functions=self.extract_functions(sql_str),
                        )
                    )

        # Check for expr(...)
        if isinstance(node.func, ast.Name) and node.func.id == "expr":
            if node.args:
                expr_str = self.extract_string_value(node.args[0])
                if expr_str:
                    self.blocks.append(
                        CodeBlock(
                            code=expr_str,
                            line_start=node.lineno,
                            line_end=getattr(node, "end_lineno", node.lineno),
                            block_type="expr",
                            functions=self.extract_functions(expr_str),
                        )
                    )

        # Check for selectExpr(...)
        if isinstance(node.func, ast.Attribute) and node.func.attr == "selectExpr":
            for arg in node.args:
                expr_str = self.extract_string_value(arg)
                if expr_str:
                    self.blocks.append(
                        CodeBlock(
                            code=expr_str,
                            line_start=node.lineno,
                            line_end=getattr(node, "end_lineno", node.lineno),
                            block_type="selectExpr",
                            functions=self.extract_functions(expr_str),
                        )
                    )

        self.generic_visit(node)

    def visit_Expr(self, node: ast.Expr):
        """Visit expression statements (often method chains)."""
        source = self.get_source(node)

        # Check if it contains any known PySpark method
        if any(f".{method}(" in source for method in self.pyspark_methods):
            self.blocks.append(
                CodeBlock(
                    code=source,
                    line_start=node.lineno,
                    line_end=getattr(node, "end_lineno", node.lineno),
                    block_type="method_chain",
                    functions=self.extract_functions(source),
                )
            )

        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign):
        """Visit assignments (df = spark.read..., etc.)."""
        # Skip simple literal assignments (no function calls)
        # This filters out: var_a = 10, my_list = ["a", "b"], config = {"k": "v"}
        if not self._has_call_nodes(node.value):
            self.generic_visit(node)
            return

        source = self.get_source(node)

        # Check if it involves PySpark operations (spark/session object or any known method)
        has_spark = "spark" in source.lower() or "session" in source.lower()
        has_pyspark_method = any(method in source for method in self.pyspark_methods)

        if has_spark or has_pyspark_method:
            self.blocks.append(
                CodeBlock(
                    code=source,
                    line_start=node.lineno,
                    line_end=getattr(node, "end_lineno", node.lineno),
                    block_type="assignment",
                    functions=self.extract_functions(source),
                )
            )

        self.generic_visit(node)


def extract_code_blocks(
    file_path: Path, pyspark_methods: set[str] | None = None
) -> list[CodeBlock]:
    """Extract PySpark code blocks from a Python file using AST."""
    try:
        source = file_path.read_text(encoding="utf-8")
        source_lines = source.splitlines()
        tree = ast.parse(source)

        extractor = PySparkExtractor(source_lines, pyspark_methods)
        extractor.visit(tree)

        return extractor.blocks
    except SyntaxError as e:
        logger.warning(f"Warning: Syntax error in {file_path}: {e}")
        return []
    except Exception as e:
        logger.warning(f"Warning: Could not parse {file_path}: {e}")
        return []


def find_pyspark_files(path: Path) -> list[Path]:
    """Find all Python files in the given path."""
    if path.is_file():
        if path.suffix == ".py":
            return [path]
        return []
    return list(path.rglob("*.py"))


def _process_single_block(
    block: CodeBlock,
    scos_rag: BaseRAG,
    api_compat: dict[str, APIInfo],
    file_path: Path,
    similarity_threshold: float,
    safe_apis: set[str] | None = None,
) -> tuple[dict | None, dict | None]:
    """
    Process a single code block for compatibility analysis.

    SNOW-3347480: Accepts safe_apis allowlist; skips RAG query if all
    functions in the block are known-safe.

    Returns:
        Tuple of (rdd_result, block_to_analyze) where:
        - rdd_result: If block is RDD, contains the final result dict
        - block_to_analyze: If block needs LLM analysis, contains preliminary data
        Both can be None if block is SCOS compatible.
    """
    # Check for RDD usage first (always 100% risk)
    is_rdd, rdd_reason = has_rdd_usage(block.code)

    if is_rdd:
        # RDD operations are not supported - 100% risk, no LLM needed
        return (
            {
                "file": str(file_path),
                "lines": f"{block.line_start}-{block.line_end}",
                "code": block.code,
                "final_risk": 1.0,
                "root_cause": rdd_reason,
                "explanation": "RDD operations are not supported in SCOS.",
                "fix": "Convert to DataFrame operations. RDD operations are not supported in SCOS.",
                "confidence": "HIGH",
            },
            None,
        )

    # Check for unsupported Spark APIs (from Snowflake docs)
    api_issues = check_unsupported_apis(block.code)

    # Check for data source issues (unsupported formats, modes, options)
    datasource_issues = check_data_source_issues(block.code)

    # Check for Spark configs that are no-ops in SCOS
    config_issues = check_config_no_ops(block.code)

    # Check for UDF serialization issues (applyInPandas/mapInPandas)
    udf_issues = check_udf_serialization_issues(block.code)

    # SNOW-3347695: Check for per-property SparkContext access patterns
    spark_context_issues = check_spark_context_properties(block.code)

    # SNOW-3347699: Check for Hadoop filesystem access patterns
    hadoop_issues = check_hadoop_patterns(block.code)

    # SNOW-3347690: Check for USE DATABASE/SCHEMA statements
    use_statement_issues = check_use_statements(block.code)

    # SNOW-3347693: Check for JVM-only library imports
    jvm_library_issues = check_jvm_library_imports(block.code)

    # SNOW-3319134: Check for ML pipeline patterns
    ml_pipeline_issues = check_ml_pipeline_patterns(block.code)

    # SNOW-3319139: Check for UDTF/UDAF patterns
    udtf_udaf_issues = check_udtf_udaf_patterns(block.code)

    # SNOW-3319141: Check for Delta Lake patterns
    delta_lake_issues = check_delta_lake_patterns(block.code)

    # SNOW-3277715: Check for lazy view re-evaluation patterns
    view_reuse_issues = check_view_reuse_patterns(block.code)

    # SNOW-3256946, SNOW-3256947, SNOW-3256949, SNOW-3256948, SNOW-3256950:
    # Check for memory anti-patterns, known issues, case sensitivity, UDF config, performance
    memory_known_issues = check_memory_and_known_issues(block.code)

    # Combine all SCOS-specific issues
    scos_issues = (
        api_issues + datasource_issues + config_issues + udf_issues
        + spark_context_issues + hadoop_issues + use_statement_issues
        + jvm_library_issues + ml_pipeline_issues + udtf_udaf_issues
        + delta_lake_issues + view_reuse_issues + memory_known_issues
    )

    # SNOW-3347480: If no rule-based issues AND all functions are in the safe
    # allowlist, skip the RAG query entirely — this block is known-compatible.
    if not scos_issues and safe_apis and is_block_safe(block.functions, safe_apis):
        global _SAFE_API_SKIPS
        _SAFE_API_SKIPS += 1
        return None, None

    # Calculate max risk from SCOS issues
    scos_risk = max((issue["risk"] for issue in scos_issues), default=0)

    # Get unified RAG prediction - use normalized code for better matching
    prediction = scos_rag.predict_failure(block.normalized_code)

    # Collect candidates from unified RAG
    candidates = []

    for p in prediction.get("similar_patterns", []):
        if p.root_cause:  # Only consider if it has a known issue
            candidates.append(
                {
                    "source": "UNIFIED_RAG",
                    "code": p.code,
                    "score": p.score,
                    "root_cause": p.root_cause,
                    "test_name": p.test_name,
                    "additional_notes": p.additional_notes,
                }
            )

    # Sort by score descending
    candidates.sort(key=lambda x: x["score"], reverse=True)

    # Filter candidates by similarity threshold (both are 0-1)
    candidates = [c for c in candidates if c["score"] >= similarity_threshold]

    # Select top matches (up to 3)
    matching_patterns = []
    failure_likelihood = 0.0

    if candidates:
        # Best match sets the base likelihood
        best_match = candidates[0]
        failure_likelihood = best_match["score"]
        matching_patterns.append(best_match)

        # Add up to 2 more if they have relatively high scores
        # (e.g., at least 85% of the best match's score)
        relative_threshold = failure_likelihood * 0.85
        for c in candidates[1:]:
            if len(matching_patterns) >= 3:
                break
            if c["score"] >= relative_threshold:
                matching_patterns.append(c)

    # If no issues detected from any source and no matching patterns above threshold,
    # skip this code block - it's considered SCOS compatible
    if not scos_issues and not matching_patterns:
        return None, None

    # Get API compatibility for functions in this block
    func_compat = []
    min_compat_score = 1.0
    for func in block.functions:
        if func in api_compat:
            info = api_compat[func]
            func_compat.append(
                {
                    "name": func,
                    "compatibility": info.compatibility,
                    "score": info.score,
                    "supported": info.is_supported,
                }
            )
            if info.score is not None and info.score < min_compat_score:
                min_compat_score = info.score

    # Calculate preliminary risk from rule-based sources (all on 0-1 scale)
    api_risk = 1.0 - min_compat_score if min_compat_score < 1.0 else 0.0
    preliminary_risk = max(failure_likelihood, api_risk, scos_risk)

    # Prepare preliminary assessment for LLM
    preliminary_assessment = {
        "scos_issues": scos_issues,
        "scos_risk": scos_risk,
        "api_risk": api_risk,
        "func_compatibility": func_compat,
        "rag_similarity": failure_likelihood,
    }

    # Return block data for batch LLM processing
    return (
        None,
        {
            "block": block,
            "matching_patterns": matching_patterns,
            "preliminary_assessment": preliminary_assessment,
            "preliminary_risk": preliminary_risk,
            "min_compat_score": min_compat_score,
            "func_compat": func_compat,
            "scos_issues": scos_issues,
            "scos_risk": scos_risk,
            "failure_likelihood": failure_likelihood,
        },
    )


# Default number of parallel workers for block processing
DEFAULT_PARALLEL_WORKERS = 8


# SNOW-3347477: Three-phase architecture — Phase 1 & 2 (extract + batch search)
def prefetch_rag_queries(
    files: list[Path],
    scos_rag: BaseRAG,
    pyspark_methods: set[str],
    safe_apis: set[str] | None = None,
    parallel_workers: int = DEFAULT_PARALLEL_WORKERS,
) -> dict[str, int]:
    """
    Pre-warm the RAG cache by extracting all queries from all files and
    executing unique queries in parallel BEFORE per-file analysis begins.

    Three-phase architecture:
      Phase 1 (EXTRACT): Parse all files, extract code blocks, collect unique
        normalized queries. Skip blocks where all functions are in safe_apis.
      Phase 2 (SEARCH): Execute all unique queries via ThreadPoolExecutor.
        Results are stored in scos_rag._cache (via search_cached).
      Phase 3 (ANALYZE): Handled by analyze_file() — reads from pre-warmed cache.

    Args:
        files: List of file paths to analyze.
        scos_rag: RAG service (results cached in-memory via BaseRAG).
        pyspark_methods: Known PySpark method names for extraction.
        safe_apis: SNOW-3347480 allowlist to skip safe patterns.
        parallel_workers: Max concurrent Cortex Search queries.

    Returns:
        Stats dict with total_blocks, unique_queries, safe_skipped, errors.
    """
    import time as _time

    # --- Phase 1: EXTRACT (CPU-only, fast) ---
    phase1_start = _time.time()
    unique_queries: set[str] = set()
    total_blocks = 0
    safe_skipped = 0

    for file_path in files:
        blocks = extract_code_blocks(file_path, pyspark_methods)
        for block in blocks:
            total_blocks += 1
            # SNOW-3347480: Skip blocks where all functions are safe
            if safe_apis and is_block_safe(block.functions, safe_apis):
                # Check RDD first — RDD blocks should not be skipped
                is_rdd, _ = has_rdd_usage(block.code)
                if not is_rdd:
                    safe_skipped += 1
                    continue
            unique_queries.add(block.normalized_code)

    phase1_time = _time.time() - phase1_start
    logger.info(
        "Phase 1 (extract): %d blocks from %d files, %d unique queries, %d safe-skipped (%.1fs)",
        total_blocks,
        len(files),
        len(unique_queries),
        safe_skipped,
        phase1_time,
    )

    if not unique_queries:
        return {
            "total_blocks": total_blocks,
            "unique_queries": 0,
            "safe_skipped": safe_skipped,
            "errors": 0,
        }

    # --- Phase 2: SEARCH (batch parallel, biggest win) ---
    phase2_start = _time.time()
    errors = 0

    def _search_one(query: str) -> str | None:
        """Execute a single search and let BaseRAG cache the result."""
        try:
            scos_rag.search_cached(query, limit=3)
            return None
        except Exception as exc:
            logger.warning("Prefetch query failed: %s", exc)
            return str(exc)

    # SNOW-3347477: Execute all unique queries in parallel
    # SNOW-3319329: Concurrency ramp — issue the first few queries serially
    # before fanning out. This is belt-and-suspenders with SCOSRemoteRAG's
    # warmup_on_init: it guarantees the Azure App Service has handled at
    # least a couple of requests sequentially before N=parallel_workers hit
    # it simultaneously, eliminating the cold-start timeout burst observed
    # in the RBI migration log.
    unique_list = list(unique_queries)
    ramp_size = min(2, len(unique_list))
    for q in unique_list[:ramp_size]:
        err = _search_one(q)
        if err is not None:
            errors += 1
    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        futures = {executor.submit(_search_one, q): q for q in unique_list[ramp_size:]}
        for future in as_completed(futures):
            err = future.result()
            if err is not None:
                errors += 1

    phase2_time = _time.time() - phase2_start
    logger.info(
        "Phase 2 (search): %d queries in %.1fs (%d errors), %d workers",
        len(unique_queries),
        phase2_time,
        errors,
        parallel_workers,
    )

    return {
        "total_blocks": total_blocks,
        "unique_queries": len(unique_queries),
        "safe_skipped": safe_skipped,
        "errors": errors,
    }


def analyze_file(
    scos_rag: BaseRAG,
    api_compat: dict[str, APIInfo],
    pyspark_methods: set[str],
    file_path: Path,
    risk_threshold: float = 0.3,  # SNOW-3347466: Default raised from 0.1 to 0.3
    session: Session | None = None,
    similarity_threshold: float = 0.55,
    llm_batch_size: int = DEFAULT_LLM_BATCH_SIZE,
    parallel_workers: int = DEFAULT_PARALLEL_WORKERS,
    safe_apis: set[str] | None = None,
) -> list[dict]:
    """
    Analyze a PySpark file for compatibility issues.

    Args:
        scos_rag: Unified RAG service for SQL and DataFrame patterns
        api_compat: API compatibility lookup
        pyspark_methods: Set of known PySpark methods
        file_path: Path to the file to analyze
        risk_threshold: Minimum risk (0-1) to report (default: 0.3 = 30%)
        session: Snowflake session (required for LLM validation)
        similarity_threshold: Minimum cosine similarity (0-1) to consider patterns relevant (default: 0.55)
        llm_batch_size: Number of code blocks to analyze per LLM call (default: 10)
        parallel_workers: Number of parallel workers for block processing (default: 8)
        safe_apis: SNOW-3347480: Set of safe API patterns to skip RAG queries for
    """
    results = []
    blocks = extract_code_blocks(file_path, pyspark_methods)

    if not blocks:
        return results

    # Phase 1: Process all blocks in parallel to collect preliminary data
    blocks_to_analyze = []  # List of block preliminary data for LLM processing

    # Process blocks in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        # Submit all block processing tasks
        future_to_block = {
            executor.submit(
                _process_single_block,
                block,
                scos_rag,
                api_compat,
                file_path,
                similarity_threshold,
                safe_apis,  # SNOW-3347480: Pass allowlist
            ): block
            for block in blocks
        }

        # Collect results as they complete
        for future in as_completed(future_to_block):
            block = future_to_block[future]
            try:
                rdd_result, block_data = future.result()

                if rdd_result is not None:
                    # RDD block - add directly to results
                    results.append(rdd_result)
                elif block_data is not None:
                    # Block needs LLM analysis
                    blocks_to_analyze.append(block_data)
                # else: block is SCOS compatible, skip it

            except Exception as e:
                logger.error(
                    f"Error processing block at lines {block.line_start}-{block.line_end}: {e}"
                )
                raise

    # Phase 2: Run batched LLM calls in parallel
    llm_results = {}  # block_id -> llm_result
    import time as _time

    if session and blocks_to_analyze:
        total_blocks = len(blocks_to_analyze)
        num_batches = (total_blocks + llm_batch_size - 1) // llm_batch_size

        logger.info(
            f"    Running LLM analysis ({parallel_workers} workers): {total_blocks} blocks in {num_batches} batch(es)..."
        )

        # Prepare all batches
        all_batch_items = []
        for batch_idx in range(0, total_blocks, llm_batch_size):
            batch = blocks_to_analyze[batch_idx : batch_idx + llm_batch_size]
            batch_num = batch_idx // llm_batch_size + 1

            batch_items = []
            for item in batch:
                block = item["block"]
                block_id = f"{block.line_start}-{block.line_end}"
                batch_items.append(
                    {
                        "block_id": block_id,
                        "input_code": block.normalized_code,
                        "matching_patterns": item["matching_patterns"],
                        "preliminary_assessment": item["preliminary_assessment"],
                    }
                )
            all_batch_items.append((batch_num, batch_items))

        # Parallel execution using ThreadPoolExecutor
        def _process_batch(args):
            batch_num, batch_items = args
            _start = _time.time()
            result = predict_compatibility_batch_with_retry(session, batch_items)
            _elapsed = _time.time() - _start
            return batch_num, result, _elapsed

        _llm_start = _time.time()
        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            futures = {
                executor.submit(_process_batch, batch): batch[0]
                for batch in all_batch_items
            }
            for future in as_completed(futures):
                batch_num, batch_result, elapsed = future.result()
                logger.info(
                    f"      Batch {batch_num}/{num_batches}: completed in {elapsed:.1f}s"
                )
                llm_results.update(batch_result)

        _total_llm_time = _time.time() - _llm_start
        logger.info(
            f"    ⏱️  Total LLM time: {_total_llm_time:.1f}s for {num_batches} batches"
        )

    # Phase 3: Build final results using LLM responses
    for item in blocks_to_analyze:
        block = item["block"]
        block_id = f"{block.line_start}-{block.line_end}"
        matching_patterns = item["matching_patterns"]
        preliminary_risk = item["preliminary_risk"]
        min_compat_score = item["min_compat_score"]
        func_compat = item["func_compat"]
        scos_issues = item["scos_issues"]
        scos_risk = item["scos_risk"]
        failure_likelihood = item["failure_likelihood"]

        # Get LLM result for this block
        llm_result = llm_results.get(block_id)

        final_risk = preliminary_risk  # Default to preliminary if LLM fails
        root_cause = None
        how_to_fix = None

        if llm_result:
            # LLM determines the final risk
            final_risk = llm_result.get("final_risk", preliminary_risk)
            root_cause = llm_result.get("root_cause")
            how_to_fix = llm_result.get("fix")

        # Fall back to rule-based root cause if LLM didn't provide one
        if not root_cause:
            if matching_patterns:
                best = matching_patterns[0]
                root_cause = best.get("root_cause")

            # If SCOS issues have higher risk, use their info
            if scos_issues and scos_risk >= failure_likelihood:
                top_issue = max(scos_issues, key=lambda x: x["risk"])
                root_cause = root_cause or top_issue["reason"]
                how_to_fix = how_to_fix or top_issue.get("how_to_fix")

        # Only report if final risk is above threshold
        if final_risk >= risk_threshold:
            explanation = (
                llm_result.get("explanation")
                if llm_result
                else f"Potential compatibility issue: {root_cause}"
            )
            confidence = (
                llm_result.get("confidence")
                if llm_result
                else ("HIGH" if final_risk >= 0.9 else "MEDIUM")
            )

            result = {
                "file": str(file_path),
                "lines": f"{block.line_start}-{block.line_end}",
                "code": block.code,
                "final_risk": final_risk,
                "root_cause": root_cause,
                "explanation": explanation,
                "fix": how_to_fix,
                "confidence": confidence,
            }

            results.append(result)

    return results


def print_json_results(results: list[dict]):
    """Print analysis results in JSON format."""
    print(json.dumps(results, indent=2))


def print_results(results: list[dict]):
    """Print analysis results."""
    if not results:
        print("\n✅ No potential issues found above threshold.")
        return

    print("\n" + "=" * 80)
    print("ANALYSIS RESULTS")
    print("=" * 80)
    print(f"Code blocks analyzed with potential issues: {len(results)}")

    for r in results:
        final_risk = r["final_risk"]
        # Choose icon based on risk
        if final_risk >= 0.7:
            risk_icon = "🔴"
        elif final_risk >= 0.3:
            risk_icon = "🟡"
        else:
            risk_icon = "🟢"

        print(f"\n{'-' * 80}")
        print(
            f"{risk_icon} {r['file']}:{r['lines']} - Final Risk: {final_risk * 100:.1f}%"
        )
        print(f"   Code: {r['code']}")

        if r.get("root_cause"):
            print(f"   Root Cause: {r['root_cause']}")

        if r.get("explanation"):
            print(f"   Explanation: {r['explanation']}")

        if r.get("fix"):
            print(f"   Fix: {r['fix']}")

        if r.get("confidence"):
            print(f"   Confidence: {r['confidence']}")


def main():
    parser = argparse.ArgumentParser(
        description="Analyze PySpark scripts for SCOS compatibility issues"
    )
    parser.add_argument(
        "path",
        type=str,
        help="Path to a PySpark file or directory containing PySpark files",
    )
    add_connectivity_args(parser)
    parser.add_argument(
        "--risk-threshold",
        "-t",
        type=float,
        default=0.3,  # SNOW-3347466: Raised from 0.1 to 0.3 to filter noisy informational EWIs
        help="Minimum risk (0-1) to report (default: 0.3 = 30%%)",
    )
    parser.add_argument(
        "--include-informational",  # SNOW-3347466: New flag to include all issues regardless of threshold
        action="store_true",
        default=False,
        help="Include all issues regardless of risk threshold (overrides --risk-threshold to 0.0)",
    )
    parser.add_argument(
        "--init",
        action="store_true",
        help="Initialize the RAG services and load CSV data",
    )
    parser.add_argument(
        "--similarity-threshold",
        "-s",
        type=float,
        default=0.55,
        help="Minimum cosine similarity [-1.0, 1.0] to consider RAG patterns relevant (default: 0.55)",
    )
    parser.add_argument(
        "--batch-size",
        "-b",
        type=int,
        default=DEFAULT_LLM_BATCH_SIZE,
        help=f"Number of code blocks to analyze per LLM call (default: {DEFAULT_LLM_BATCH_SIZE})",
    )
    parser.add_argument(
        "--parallel-workers",
        "-p",
        type=int,
        default=DEFAULT_PARALLEL_WORKERS,
        help=f"Number of parallel workers for block processing (default: {DEFAULT_PARALLEL_WORKERS})",
    )
    parser.add_argument(
        "--output-format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    args = parser.parse_args()

    # SNOW-3347466: Override risk threshold when --include-informational is used
    if args.include_informational:
        args.risk_threshold = 0.0

    # Configure logging to stderr so it doesn't interfere with stdout (text/json) output
    # Set root logger to WARNING to suppress noisy library logs
    logging.basicConfig(level=logging.WARNING, format="%(message)s", stream=sys.stderr)

    # Set this script's logger to INFO to see our own messages
    logger.setLevel(logging.INFO)

    path = Path(args.path).expanduser()
    if not path.exists():
        logger.error(f"Error: Path does not exist: {path}")
        sys.exit(1)

    # Find PySpark files
    files = find_pyspark_files(path)
    logger.info(f"Found {len(files)} Python file(s) to analyze")

    # Load API compatibility data
    compat_csv = DATA_DIR / "api_compatibility.csv"
    logger.info(f"\nLoading API compatibility data from {compat_csv}...")
    api_compat, pyspark_methods = load_api_compatibility(compat_csv)
    logger.info(
        f"Loaded {len(api_compat)} API entries, {len(pyspark_methods)} methods/functions"
    )

    # SNOW-3347480: Load safe-API allowlist
    safe_apis = load_safe_apis()

    # Connect to Snowflake
    session = open_session(args.connection)

    # Initialize RAG backend
    scos_rag: BaseRAG = build_rag(session, args.rag_backend)

    # Load data if --init flag is set (only applicable for cortex backend)
    if args.rag_backend == "cortex" and args.init:
        scos_rag.init()
        logger.info("Loading SCOS RAG data from data/...")
        total_count = 0

        rag_files = [
            "df_test_rca_normalized.csv",
            "sql_test_rca_normalized.csv",
            "expectation_tests_xfail_rca_normalized.csv",
            "jira_rca_normalized.csv",
            # SNOW-3347463: Databricks compatibility patterns
            "dbx_compat_rca_normalized.csv",
            # SNOW-3319145: ML, UDTF/UDAF, and Delta Lake patterns
            "ml_compat_rca_normalized.csv",
            "udtf_udaf_compat_rca_normalized.csv",
            "delta_lake_compat_rca_normalized.csv",
        ]
        for csv_file in rag_files:
            count = scos_rag.upload_csv(csv_file)
            logger.info(f"  Loaded {count} records from {csv_file}")
            total_count += count

        logger.info(f"  Total: {total_count} failure records loaded")

    # Analyze files
    logger.info(
        f"\nAnalyzing files (risk threshold: {args.risk_threshold * 100:.2f}%, similarity: {args.similarity_threshold}, batch size: {args.batch_size}, workers: {args.parallel_workers})..."
    )

    # SNOW-3347477: Phase 1 & 2 — Extract all queries and pre-warm the RAG cache
    # in parallel BEFORE per-file analysis (Phase 3).
    if len(files) > 1:
        logger.info("\n--- Three-phase RAG pipeline (SNOW-3347477) ---")
        prefetch_stats = prefetch_rag_queries(
            files,
            scos_rag,
            pyspark_methods,
            safe_apis=safe_apis,
            parallel_workers=args.parallel_workers,
        )
        logger.info(
            "Prefetch complete: %d unique queries cached, %d safe-API skips",
            prefetch_stats["unique_queries"],
            prefetch_stats["safe_skipped"],
        )

    # Phase 3: Per-file analysis (RAG calls hit pre-warmed cache)
    all_results = []
    for file_path in files:
        logger.info(f"  📄 {file_path.name}")
        if file_path.suffix == ".ipynb":
            cells = extract_notebook_code(str(file_path))
            for cell_info in cells:
                # Analyze cell_info["source"] as Python code
                # Include cell_info["cell_index"] in issue metadata
                pass
        else:
            results = analyze_file(
                scos_rag,
                api_compat,
                pyspark_methods,
                file_path,
                risk_threshold=args.risk_threshold,
                session=session,
                similarity_threshold=args.similarity_threshold,
                llm_batch_size=args.batch_size,
                parallel_workers=args.parallel_workers,
                safe_apis=safe_apis,
            )
            all_results.extend(results)

    # Sort by final risk (highest first)
    all_results = sorted(all_results, key=lambda x: x["final_risk"], reverse=True)

    # SNOW-3347479: Log RAG cache statistics
    scos_rag.log_cache_stats()

    # SNOW-3347480: Log safe-API skip statistics
    total_blocks_processed = _SAFE_API_SKIPS + (scos_rag.cache_stats["hits"] + scos_rag.cache_stats["misses"])
    if total_blocks_processed > 0:
        logger.info(
            "Skipped %d safe-API queries (%.1f%% of total)",
            _SAFE_API_SKIPS,
            _SAFE_API_SKIPS / (total_blocks_processed + _SAFE_API_SKIPS) * 100,
        )

    # Print results
    if args.output_format == "json":
        print_json_results(all_results)
    else:
        print_results(all_results)

    # Cleanup
    session.close()


if __name__ == "__main__":
    main()
