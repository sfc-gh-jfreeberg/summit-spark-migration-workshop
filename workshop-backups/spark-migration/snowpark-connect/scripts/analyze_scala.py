# flake8: noqa: T201

"""
SCOS Migration Agent - Scala Spark Compatibility Analyzer

Analyze Scala Spark scripts for potential SCOS compatibility issues.
Since Scala cannot be parsed with Python's ast module, this analyzer
uses regex-based pattern detection and LLM validation.

Produces the same JSON output format as analyze_pyspark.py.

Usage:
    python analyze_scala.py --path /path/to/script.scala
    python analyze_scala.py --path /path/to/scripts/
"""

import argparse
import csv
import json
import logging
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

from code_normalization import normalize_code_lightweight
from rag import BaseRAG
from scos_session import add_connectivity_args, build_rag, open_session
from snowflake.cortex import CompleteOptions, complete as cortex_complete
from snowflake.snowpark import Session

logger = logging.getLogger(__name__)

DEFAULT_LLM_MODEL = "claude-opus-4-5"
DEFAULT_LLM_BATCH_SIZE = 5
DEFAULT_PARALLEL_WORKERS = 8

DATA_DIR = Path(__file__).parent / "data"

PROMPT_PREDICT_COMPATIBILITY_BATCH = """
You are analyzing multiple Scala Spark code blocks for compatibility issues when running on Snowflake SCOS (Snowpark Connect for Spark).
Your goal is to analyze each code block and determine if it will actually fail on SCOS.

## INPUT DATA
You are provided with {num_blocks} code blocks. Each block contains:
1. `block_id`: Unique identifier.
2. `input_code`: The Scala Spark code snippet to analyze.
3. `preliminary_assessment`: Rule-based warnings (e.g., "API X is unsupported").
4. `matching_patterns`: Similar failing test cases from our database.

## ANALYSIS PROCESS (Apply to EACH block)
1. **Analyze Input**: Understand the intent and syntax of the `input_code`.
2. **Verify RAG Matches**: Compare `input_code` with `matching_patterns`.
   - Do the failing patterns share the *exact same* root cause as the input?
3. **Verify Rule-Based Warnings**: Check if the `preliminary_assessment` is valid.

## RISK SCORING RULES:
- If the similar test cases use DIFFERENT operations/patterns → final_risk 0.0 to 0.1
- If there are NO compatibility issues → final_risk 0.0
- If similar test cases use the SAME problematic pattern → final_risk 0.5 to 1.0
- Only assign high risk (>0.5) if confident the code will ACTUALLY fail
- If no similar test cases but SCOS Issues Risk > 0, use it as final_risk

BE CONSISTENT: If explanation says "should work correctly", final_risk MUST be < 0.1

## CODE BLOCKS TO ANALYZE

{code_blocks_text}

## OUTPUT FORMAT
Return ONLY a valid JSON array with EXACTLY {num_blocks} items (one per block, in order).
No text before or after the JSON.

[
    {{
        "block_id": "<the block_id from the input>",
        "analysis_thought_process": "<Step-by-step reasoning>",
        "final_risk": <0.0-1.0 float>,
        "root_cause": "<root cause or null if safe>",
        "explanation": "<1-2 sentence summary>",
        "fix": "<specific fix or null>",
        "confidence": "<HIGH|MEDIUM|LOW>"
    }},
    ...
]
"""

# Scala RDD access patterns
RDD_PATTERNS = [
    r"\.sparkContext",
    r"\.rdd\b",
    r"import\s+org\.apache\.spark\.rdd",
    r"import\s+org\.apache\.spark\.SparkContext",
    r"\bsc\.parallelize\b",
    r"\bsc\.textFile\b",
    r"\bsc\.wholeTextFiles\b",
    r"\bsc\.hadoopFile\b",
    r"\bsc\.hadoopRDD\b",
    r"\bsc\.newAPIHadoopFile\b",
    r"\bsc\.newAPIHadoopRDD\b",
    r"\bsc\.sequenceFile\b",
    r"\bsc\.objectFile\b",
    r"\bsc\.emptyRDD\b",
    r"\bnew\s+SparkContext\b",
]

# Scala RDD method names
RDD_METHODS = {
    "map", "flatMap", "filter", "reduce", "reduceByKey", "groupByKey",
    "sortByKey", "sortBy", "join", "leftOuterJoin", "rightOuterJoin",
    "fullOuterJoin", "cogroup", "cartesian", "pipe", "foreach",
    "foreachPartition", "collect", "count", "first", "take", "takeSample",
    "takeOrdered", "saveAsTextFile", "saveAsSequenceFile", "saveAsObjectFile",
    "countByKey", "countByValue", "aggregate", "fold", "glom",
    "mapPartitions", "mapPartitionsWithIndex", "zip", "zipWithIndex",
    "zipWithUniqueId", "keyBy", "keys", "values", "lookup", "top",
    "mapValues", "flatMapValues", "combineByKey", "aggregateByKey",
    "foldByKey", "sampleByKey",
}

# Unsupported Scala imports
UNSUPPORTED_IMPORTS = {
    "org.apache.spark.ml": {
        "risk": 1.0,
        "reason": "Spark ML (org.apache.spark.ml) is not supported in SCOS",
        "category": "Unsupported Module",
    },
    "org.apache.spark.mllib": {
        "risk": 1.0,
        "reason": "Spark MLlib (org.apache.spark.mllib) is not supported in SCOS",
        "category": "Unsupported Module",
    },
    "org.apache.spark.streaming": {
        "risk": 1.0,
        "reason": "Spark Streaming (org.apache.spark.streaming) is not supported in SCOS",
        "category": "Unsupported Module",
    },
    "org.apache.spark.graphx": {
        "risk": 1.0,
        "reason": "GraphX (org.apache.spark.graphx) is not supported in SCOS",
        "category": "Unsupported Module",
    },
    "org.apache.spark.sql.catalyst": {
        "risk": 1.0,
        "reason": "Spark Catalyst internals (org.apache.spark.sql.catalyst) are not available via Spark Connect — replace with custom types",
        "category": "Unsupported Module",
    },
    "org.apache.hadoop": {
        "risk": 0.9,
        "reason": "Hadoop APIs (org.apache.hadoop) are not available in SCOS — remove HDFS/FileSystem usage and use Snowflake stages",
        "category": "Unsupported Module",
    },
    "org.apache.spark.sql.hive": {
        "risk": 1.0,
        "reason": "Hive integration (org.apache.spark.sql.hive) is not available in SCOS",
        "category": "Unsupported Module",
    },
    "com.hortonworks.spark.sql.hive": {
        "risk": 1.0,
        "reason": "Hive Warehouse Connector is not available in SCOS",
        "category": "Unsupported Module",
    },
}

UNSUPPORTED_FORMATS = {
    "avro": {"risk": 1.0, "reason": "Avro format is not supported in SCOS", "category": "Unsupported Format"},
    "orc": {"risk": 1.0, "reason": "ORC format is not supported in SCOS", "category": "Unsupported Format"},
    "delta": {"risk": 1.0, "reason": "Delta format is not supported in SCOS", "category": "Unsupported Format"},
}

NO_OP_APIS = {
    "hint": {"risk": 0.2, "reason": "DataFrame.hint() is ignored in SCOS", "category": "No-Op API"},
    "repartition": {"risk": 0.2, "reason": "DataFrame.repartition() is a no-op in SCOS", "category": "No-Op API"},
    "coalesce": {"risk": 0.2, "reason": "DataFrame.coalesce() is a no-op in SCOS", "category": "No-Op API"},
}

HIVE_DDL_PATTERNS = [
    (r"""(?i)\bspark\.sql\s*\(\s*["']MSCK\s+REPAIR\s+TABLE""", "MSCK REPAIR TABLE is Hive-specific and not supported in SCOS/Snowflake"),
    (r"""(?i)\bspark\.sql\s*\(\s*["']ALTER\s+TABLE\s+\S+\s+RECOVER\s+PARTITIONS""", "ALTER TABLE RECOVER PARTITIONS is Hive-specific and not supported in SCOS"),
    (r"""(?i)\bspark\.sql\s*\(\s*["']CREATE\s+(EXTERNAL\s+)?TABLE""", "Hive CREATE TABLE DDL may not be compatible with SCOS — use Snowflake SQL or DataFrame API"),
    (r"\.hadoopConfiguration\b", "sparkContext.hadoopConfiguration is not available in Spark Connect"),
    (r"\benableHiveSupport\b", "enableHiveSupport() is not available in SCOS — Hive metastore is not accessible"),
    (r"HiveContext\b", "HiveContext is not available in SCOS"),
]


@dataclass
class ScalaCodeBlock:
    code: str
    line_start: int
    line_end: int
    block_type: str
    functions: list[str] = field(default_factory=list)

    @property
    def normalized_code(self) -> str:
        return normalize_code_lightweight(self.code)


def find_scala_files(path: Path) -> list[Path]:
    if path.is_file():
        if path.suffix == ".scala":
            return [path]
        return []
    return list(path.rglob("*.scala"))


def has_rdd_usage(code: str) -> tuple[bool, str | None]:
    for pattern in RDD_PATTERNS:
        if re.search(pattern, code):
            return True, f"Uses RDD pattern '{pattern}' which is not supported in SCOS"
    code_lower = code.lower()
    if ".rdd" in code_lower or "sparkcontext" in code_lower:
        for method in RDD_METHODS:
            if f".{method}(" in code:
                return True, f"RDD operation '.{method}()' is not supported in SCOS"
    return False, None


def check_unsupported_imports_scala(code: str) -> list[dict]:
    issues = []
    for module, info in UNSUPPORTED_IMPORTS.items():
        if f"import {module}" in code:
            issues.append({
                "api": module,
                "risk": info["risk"],
                "reason": info["reason"],
                "category": info["category"],
            })
    return issues


def check_unsupported_formats_scala(code: str) -> list[dict]:
    issues = []
    code_lower = code.lower()
    for fmt, info in UNSUPPORTED_FORMATS.items():
        patterns = [f'.format("{fmt}")', f".format('{fmt}')"]
        for p in patterns:
            if p.lower() in code_lower:
                issues.append({
                    "format": fmt,
                    "risk": info["risk"],
                    "reason": info["reason"],
                    "category": info["category"],
                })
                break
    return issues


def check_noop_apis_scala(code: str) -> list[dict]:
    issues = []
    for method, info in NO_OP_APIS.items():
        if f".{method}(" in code:
            issues.append({
                "api": method,
                "risk": info["risk"],
                "reason": info["reason"],
                "category": info["category"],
            })
    return issues


def check_hive_ddl_patterns_scala(code: str) -> list[dict]:
    issues = []
    for pattern, reason in HIVE_DDL_PATTERNS:
        if re.search(pattern, code):
            issues.append({
                "api": pattern,
                "risk": 0.9,
                "reason": reason,
                "category": "Unsupported Module",
            })
    return issues


def check_udf_patterns_scala(code: str) -> list[dict]:
    issues = []
    udf_patterns = [r"\.udf\b", r"spark\.udf\.register", r"functions\.udf\("]
    for p in udf_patterns:
        if re.search(p, code):
            issues.append({
                "api": "UDF",
                "risk": 0.5,
                "reason": (
                    "UDFs in Scala may have serialization issues on Snowflake's server-side worker. "
                    "Ensure all dependencies are self-contained or available in Snowflake's runtime."
                ),
                "category": "UDF Serialization",
            })
            break
    return issues


def extract_scala_blocks(file_path: Path) -> list[ScalaCodeBlock]:
    """Extract code blocks from a Scala file using line-based heuristics."""
    try:
        source = file_path.read_text(encoding="utf-8")
    except OSError:
        return []

    lines = source.splitlines()
    blocks = []

    spark_keywords = {
        "spark.", "session.", ".read", ".write", ".sql(", ".select(",
        ".filter(", ".where(", ".groupBy(", ".agg(", ".join(",
        ".withColumn(", ".drop(", ".show(", ".collect(",
        ".format(", ".load(", ".save(", ".option(",
        "SparkSession", "SparkContext", "SparkConf",
        "import org.apache.spark",
    }

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if not stripped or stripped.startswith("//") or stripped.startswith("/*"):
            i += 1
            continue

        if any(kw in line for kw in spark_keywords):
            block_start = i
            block_lines = [line]

            # Accumulate continuation lines (lines ending with . or { or lines that are chained)
            j = i + 1
            while j < len(lines):
                next_line = lines[j].strip()
                prev_stripped = block_lines[-1].strip()
                if (
                    prev_stripped.endswith(".")
                    or prev_stripped.endswith(",")
                    or prev_stripped.endswith("{")
                    or prev_stripped.endswith("(")
                    or next_line.startswith(".")
                    or next_line.startswith(")")
                ):
                    block_lines.append(lines[j])
                    j += 1
                else:
                    break

            code = "\n".join(block_lines)
            funcs = list(set(re.findall(r"\.(\w+)\s*\(", code)))

            blocks.append(ScalaCodeBlock(
                code=code,
                line_start=block_start + 1,
                line_end=block_start + len(block_lines),
                block_type="statement",
                functions=funcs,
            ))
            i = j
        else:
            i += 1

    return blocks


def _process_single_block(
    block: ScalaCodeBlock,
    scos_rag: BaseRAG,
    file_path: Path,
    similarity_threshold: float,
) -> tuple[dict | None, dict | None]:
    is_rdd, rdd_reason = has_rdd_usage(block.code)
    if is_rdd:
        return (
            {
                "file": str(file_path),
                "lines": f"{block.line_start}-{block.line_end}",
                "code": block.code,
                "final_risk": 1.0,
                "root_cause": rdd_reason,
                "explanation": "RDD operations are not supported in SCOS.",
                "fix": "Convert to DataFrame operations.",
                "confidence": "HIGH",
            },
            None,
        )

    import_issues = check_unsupported_imports_scala(block.code)
    format_issues = check_unsupported_formats_scala(block.code)
    noop_issues = check_noop_apis_scala(block.code)
    udf_issues = check_udf_patterns_scala(block.code)
    hive_issues = check_hive_ddl_patterns_scala(block.code)

    scos_issues = import_issues + format_issues + noop_issues + udf_issues + hive_issues
    scos_risk = max((i["risk"] for i in scos_issues), default=0)

    prediction = scos_rag.predict_failure(block.normalized_code)
    candidates = []
    for p in prediction.get("similar_patterns", []):
        if p.root_cause:
            candidates.append({
                "source": "UNIFIED_RAG",
                "code": p.code,
                "score": p.score,
                "root_cause": p.root_cause,
                "test_name": p.test_name,
                "additional_notes": p.additional_notes,
            })

    candidates.sort(key=lambda x: x["score"], reverse=True)
    candidates = [c for c in candidates if c["score"] >= similarity_threshold]

    matching_patterns = []
    failure_likelihood = 0.0
    if candidates:
        best_match = candidates[0]
        failure_likelihood = best_match["score"]
        matching_patterns.append(best_match)
        relative_threshold = failure_likelihood * 0.85
        for c in candidates[1:]:
            if len(matching_patterns) >= 3:
                break
            if c["score"] >= relative_threshold:
                matching_patterns.append(c)

    if not scos_issues and not matching_patterns:
        return None, None

    preliminary_risk = max(failure_likelihood, scos_risk)
    preliminary_assessment = {
        "scos_issues": scos_issues,
        "scos_risk": scos_risk,
        "rag_similarity": failure_likelihood,
    }

    return (
        None,
        {
            "block": block,
            "matching_patterns": matching_patterns,
            "preliminary_assessment": preliminary_assessment,
            "preliminary_risk": preliminary_risk,
            "scos_issues": scos_issues,
            "scos_risk": scos_risk,
            "failure_likelihood": failure_likelihood,
        },
    )


def _build_assessment_text(preliminary_assessment: dict) -> str:
    parts = []
    scos_issues = preliminary_assessment.get("scos_issues", [])
    if scos_issues:
        parts.append("SCOS Compatibility Issues:")
        for issue in scos_issues:
            name = issue.get("api") or issue.get("format", "unknown")
            parts.append(f"  - {name}: {issue['reason']} (Risk: {issue['risk'] * 100:.0f}%)")
    scos_risk = preliminary_assessment.get("scos_risk", 0)
    if scos_risk > 0:
        parts.append(f"\nSCOS Issues Risk: {scos_risk * 100:.0f}%")
    return "\n".join(parts) if parts else "No rule-based issues detected."


def _build_patterns_text(matching_patterns: list[dict]) -> str:
    if not matching_patterns:
        return "No similar failing test cases found above similarity threshold."
    parts = []
    for i, p in enumerate(matching_patterns, 1):
        parts.append(
            f"TEST CASE #{i} (Cosine similarity: {p.get('score', 0.0):.1%})\n"
            f"Test Name: {p.get('test_name', 'N/A')}\n"
            f"Code/SQL:\n```\n{p.get('code', '')}\n```\n"
            f"Root Cause: {p.get('root_cause', 'N/A')}\n"
            f"Additional Notes: {p.get('additional_notes', 'N/A')}"
        )
    return "\n\n".join(parts)


def predict_compatibility_batch(
    session: Session,
    batch_items: list[dict],
    model: str = DEFAULT_LLM_MODEL,
) -> dict[str, dict]:
    if not batch_items:
        return {}

    code_blocks_parts = []
    for item in batch_items:
        assessment_text = _build_assessment_text(item.get("preliminary_assessment", {}))
        patterns_text = _build_patterns_text(item.get("matching_patterns", []))
        code_blocks_parts.append(
            f"### BLOCK {item['block_id']}\n\n```scala\n{item['input_code']}\n```\n\n"
            f"**Preliminary Assessment:**\n{assessment_text}\n\n"
            f"**Similar Failing Test Cases:**\n{patterns_text}\n\n---"
        )

    prompt = PROMPT_PREDICT_COMPATIBILITY_BATCH.format(
        code_blocks_text="\n\n".join(code_blocks_parts),
        num_blocks=len(batch_items),
    )

    try:
        options = CompleteOptions(temperature=0.0)
        response = cortex_complete(model, prompt, options=options, session=session)
        response = response.strip()
        if response.startswith("```"):
            lines = response.split("\n")
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            response = "\n".join(lines)

        results_list = json.loads(response)
        results_dict = {}
        for result in results_list:
            bid = result.get("block_id")
            if bid:
                results_dict[bid] = result
        return results_dict
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON from LLM: {response[:500]}...\nError: {e}")
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
                logger.error("Batch LLM failed after %d attempts: %s — returning empty results", max_retries, exc)
                return {}


def analyze_file(
    scos_rag: BaseRAG,
    file_path: Path,
    risk_threshold: float = 0.1,
    session: Session | None = None,
    similarity_threshold: float = 0.55,
    llm_batch_size: int = DEFAULT_LLM_BATCH_SIZE,
    parallel_workers: int = DEFAULT_PARALLEL_WORKERS,
) -> list[dict]:
    results = []
    blocks = extract_scala_blocks(file_path)
    if not blocks:
        return results

    blocks_to_analyze = []

    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        future_to_block = {
            executor.submit(
                _process_single_block, block, scos_rag, file_path, similarity_threshold
            ): block
            for block in blocks
        }
        for future in as_completed(future_to_block):
            block = future_to_block[future]
            try:
                rdd_result, block_data = future.result()
                if rdd_result is not None:
                    results.append(rdd_result)
                elif block_data is not None:
                    blocks_to_analyze.append(block_data)
            except Exception as e:
                logger.error(f"Error processing block at lines {block.line_start}-{block.line_end}: {e}")
                raise

    import time as _time
    llm_results = {}

    if session and blocks_to_analyze:
        total_blocks = len(blocks_to_analyze)
        num_batches = (total_blocks + llm_batch_size - 1) // llm_batch_size
        logger.info(f"    Running LLM analysis: {total_blocks} blocks in {num_batches} batch(es)...")

        all_batch_items = []
        for batch_idx in range(0, total_blocks, llm_batch_size):
            batch = blocks_to_analyze[batch_idx:batch_idx + llm_batch_size]
            batch_num = batch_idx // llm_batch_size + 1
            batch_items = []
            for item in batch:
                block = item["block"]
                block_id = f"{block.line_start}-{block.line_end}"
                batch_items.append({
                    "block_id": block_id,
                    "input_code": block.normalized_code,
                    "matching_patterns": item["matching_patterns"],
                    "preliminary_assessment": item["preliminary_assessment"],
                })
            all_batch_items.append((batch_num, batch_items))

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
                logger.info(f"      Batch {batch_num}/{num_batches}: completed in {elapsed:.1f}s")
                llm_results.update(batch_result)

    for item in blocks_to_analyze:
        block = item["block"]
        block_id = f"{block.line_start}-{block.line_end}"
        matching_patterns = item["matching_patterns"]
        preliminary_risk = item["preliminary_risk"]
        scos_issues = item["scos_issues"]
        scos_risk = item["scos_risk"]
        failure_likelihood = item["failure_likelihood"]

        llm_result = llm_results.get(block_id)
        final_risk = preliminary_risk
        root_cause = None
        how_to_fix = None

        if llm_result:
            final_risk = llm_result.get("final_risk", preliminary_risk)
            root_cause = llm_result.get("root_cause")
            how_to_fix = llm_result.get("fix")

        if not root_cause:
            if matching_patterns:
                root_cause = matching_patterns[0].get("root_cause")
            if scos_issues and scos_risk >= failure_likelihood:
                top_issue = max(scos_issues, key=lambda x: x["risk"])
                root_cause = root_cause or top_issue["reason"]
                how_to_fix = how_to_fix or top_issue.get("how_to_fix")

        if final_risk >= risk_threshold:
            explanation = (
                llm_result.get("explanation") if llm_result
                else f"Potential compatibility issue: {root_cause}"
            )
            confidence = (
                llm_result.get("confidence") if llm_result
                else ("HIGH" if final_risk >= 0.9 else "MEDIUM")
            )
            results.append({
                "file": str(file_path),
                "lines": f"{block.line_start}-{block.line_end}",
                "code": block.code,
                "final_risk": final_risk,
                "root_cause": root_cause,
                "explanation": explanation,
                "fix": how_to_fix,
                "confidence": confidence,
            })

    return results


def print_json_results(results: list[dict]):
    print(json.dumps(results, indent=2))


def print_results(results: list[dict]):
    if not results:
        print("\nNo potential issues found above threshold.")
        return

    print("\n" + "=" * 80)
    print("SCALA SPARK ANALYSIS RESULTS")
    print("=" * 80)
    print(f"Code blocks with potential issues: {len(results)}")

    for r in results:
        final_risk = r["final_risk"]
        print(f"\n{'-' * 80}")
        print(f"  {r['file']}:{r['lines']} - Risk: {final_risk * 100:.1f}%")
        print(f"   Code: {r['code'][:200]}")
        if r.get("root_cause"):
            print(f"   Root Cause: {r['root_cause']}")
        if r.get("fix"):
            print(f"   Fix: {r['fix']}")
        if r.get("confidence"):
            print(f"   Confidence: {r['confidence']}")


def main():
    parser = argparse.ArgumentParser(description="Analyze Scala Spark scripts for SCOS compatibility issues")
    parser.add_argument("--path", type=str, required=True, help="Path to Scala file or directory")
    add_connectivity_args(parser)
    parser.add_argument("--risk-threshold", "-t", type=float, default=0.1, help="Minimum risk (0-1) to report")
    parser.add_argument("--similarity-threshold", "-s", type=float, default=0.55, help="Minimum cosine similarity")
    parser.add_argument("--batch-size", "-b", type=int, default=DEFAULT_LLM_BATCH_SIZE, help="Blocks per LLM call")
    parser.add_argument("--parallel-workers", "-p", type=int, default=DEFAULT_PARALLEL_WORKERS, help="Parallel workers")
    parser.add_argument("--output-format", choices=["text", "json"], default="text", help="Output format")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(message)s", stream=sys.stderr)
    logger.setLevel(logging.INFO)

    path = Path(args.path).expanduser()
    if not path.exists():
        logger.error(f"Error: Path does not exist: {path}")
        sys.exit(1)

    files = find_scala_files(path)
    logger.info(f"Found {len(files)} Scala file(s) to analyze")

    session = open_session(args.connection)
    scos_rag: BaseRAG = build_rag(session, args.rag_backend)

    logger.info(
        f"\nAnalyzing Scala files (risk: {args.risk_threshold * 100:.0f}%, "
        f"similarity: {args.similarity_threshold}, batch: {args.batch_size}, "
        f"workers: {args.parallel_workers})..."
    )

    all_results = []
    for file_path in files:
        logger.info(f"  {file_path.name}")
        results = analyze_file(
            scos_rag, file_path,
            risk_threshold=args.risk_threshold,
            session=session,
            similarity_threshold=args.similarity_threshold,
            llm_batch_size=args.batch_size,
            parallel_workers=args.parallel_workers,
        )
        all_results.extend(results)

    all_results = sorted(all_results, key=lambda x: x["final_risk"], reverse=True)

    if args.output_format == "json":
        print_json_results(all_results)
    else:
        print_results(all_results)

    session.close()


if __name__ == "__main__":
    main()
