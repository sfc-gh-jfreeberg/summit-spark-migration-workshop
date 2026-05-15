"""
Shared connectivity helpers for SCOS analyzer scripts.

Centralizes the Snowflake session bootstrap and RAG backend selection so that
``analyze_pyspark.py`` and ``analyze_scala.py`` (and any future analyzer) stay
in sync on CLI flags and backend fallback behavior.
"""

from __future__ import annotations

import argparse
import logging
import sys

from rag import BaseRAG, SCOSCortexRAG, SCOSRemoteRAG, SCOSRemoteRAGConfig

from snowflake.snowpark import Session

logger = logging.getLogger(__name__)


def add_connectivity_args(parser: argparse.ArgumentParser) -> None:
    """Add the shared ``--connection`` and ``--rag-backend`` flags to *parser*."""
    parser.add_argument(
        "--connection",
        type=str,
        default="default",
        help="Snowflake connection name (default: default)",
    )
    parser.add_argument(
        "--rag-backend",
        choices=["remote", "cortex"],
        default="remote",
        help=(
            "RAG backend: 'remote' (WebAPI, default; falls back to Cortex Search "
            "if the WebAPI is unreachable) or 'cortex' (Snowflake Cortex Search "
            "directly — no fallback)"
        ),
    )


def open_session(connection_name: str) -> Session:
    """Open a Snowpark session for *connection_name* or exit on failure."""
    logger.info("\nConnecting to Snowflake (connection: %s)...", connection_name)
    try:
        return Session.builder.config("connection_name", connection_name).create()
    except Exception as exc:
        logger.error("Error connecting to Snowflake: %s", exc)
        logger.info("\nMake sure you have a valid connection configured.")
        sys.exit(1)


def _fetch_snowflake_identifiers(
    session: Session,
) -> tuple[str | None, str | None, str | None]:
    """Return ``(sessionId, user, account)`` for WebAPI auth, or ``(None, None, None)`` on failure."""
    try:
        row = session.sql(
            "SELECT CURRENT_SESSION(), CURRENT_USER(), CURRENT_ACCOUNT()"
        ).collect()[0]
        return str(row[0]), str(row[1]), str(row[2])
    except Exception as exc:  # pragma: no cover — surfaces as a WebAPI failure downstream
        logger.warning("Could not fetch Snowflake session identifiers: %s", exc)
        return None, None, None


def _build_remote_rag(session: Session) -> SCOSRemoteRAG:
    """Build a :class:`SCOSRemoteRAG` and probe connectivity with a single auth call."""
    sess_id, sf_user, sf_account = _fetch_snowflake_identifiers(session)
    if not all([sess_id, sf_user, sf_account]):
        raise RuntimeError(
            "Could not resolve Snowflake session identifiers required for WebAPI auth"
        )
    cfg = SCOSRemoteRAGConfig(
        snowflake_session_id=sess_id,
        snowflake_user=sf_user,
        snowflake_account=sf_account,
    )
    rag = SCOSRemoteRAG(config=cfg)
    # Connectivity + auth probe: hits /auth/token once.
    rag._ensure_authenticated()
    return rag


def build_rag(session: Session, backend: str) -> BaseRAG:
    """Build a RAG backend using SNOW-3319329's inverted-priority policy.

    - ``backend == "cortex"``: explicit user choice. Uses
      :meth:`SCOSCortexRAG.discover` against the given *session* with no
      fallback to remote.
    - ``backend == "remote"`` (default): try the WebAPI first; on any failure
      fall back to Cortex Search via :meth:`SCOSCortexRAG.discover`. If both
      backends are unavailable, log an error and :func:`sys.exit(1)`.
    """
    if backend == "cortex":
        # Explicit user choice — no fallback to remote.
        scos_rag: BaseRAG = SCOSCortexRAG.discover(session)
        logger.info("Using Cortex Search RAG backend (explicit --rag-backend cortex)")
        return scos_rag

    # Default: try remote WebAPI first, fall back to Cortex Search on failure.
    try:
        scos_rag = _build_remote_rag(session)
        logger.info("Using remote WebAPI RAG backend")
        return scos_rag
    except Exception as exc:
        logger.warning(
            "Remote WebAPI RAG unavailable (%s) — falling back to Cortex Search",
            exc,
        )
        try:
            scos_rag = SCOSCortexRAG.discover(session)
            logger.info("Using Cortex Search RAG backend (fallback)")
            return scos_rag
        except Exception as cex:
            logger.error(
                "Neither remote WebAPI nor Cortex Search are available: %s",
                cex,
            )
            sys.exit(1)
