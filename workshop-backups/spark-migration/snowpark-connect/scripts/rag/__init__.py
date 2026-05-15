"""
SCOS Migration Agent - RAG Module

Provides RAG services for finding similar failing PySpark code and SQL patterns.

Two backends are available:
  - SCOSCortexRAG: Snowflake Cortex Search
  - SCOSRemoteRAG: Remote HTTP endpoint

Both conform to the BaseRAG interface.
"""

from .base import BaseRAG, SCOSSearchResult
from .scos_rag import SCOSCortexRAG, SCOSRAGConfig
from .scos_remote_rag import SCOSRemoteRAG, SCOSRemoteRAGConfig

__all__ = [
    "BaseRAG",
    "SCOSSearchResult",
    "SCOSCortexRAG",
    "SCOSRAGConfig",
    "SCOSRemoteRAG",
    "SCOSRemoteRAGConfig",
]
