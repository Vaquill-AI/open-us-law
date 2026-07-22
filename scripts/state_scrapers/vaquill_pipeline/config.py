"""Env-driven config for the Vaquill state-statutes chunk pipeline."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
    # also try the main backend repo .env (sibling clone)
    load_dotenv(Path(__file__).resolve().parents[2] / "vaquill-qdrant-python" / ".env")
except ImportError:
    pass


@dataclass(frozen=True)
class Settings:
    # Output dir for chunk JSONL files (one file per state).
    chunks_dir: Path = Path(
        os.environ.get(
            "VAQUILL_CHUNKS_DIR",
            str(Path(__file__).resolve().parent.parent / "data" / "state_chunks"),
        )
    )

    # Chunking knobs (kept in sync with scripts/us_corpus/chunk_and_ingest.py).
    chunk_size_tokens: int = int(os.environ.get("VAQUILL_CHUNK_SIZE_TOKENS", "1000"))
    chunk_overlap_tokens: int = int(os.environ.get("VAQUILL_CHUNK_OVERLAP_TOKENS", "100"))
    min_chunk_size_tokens: int = int(os.environ.get("VAQUILL_MIN_CHUNK_TOKENS", "100"))
    approx_chars_per_token: int = int(os.environ.get("VAQUILL_CHARS_PER_TOKEN", "4"))

    # Voyage model the chunks are intended for (informational only; chunks are
    # embed-model-agnostic until embed_and_upsert.py runs).
    target_voyage_model: str = os.environ.get("VAQUILL_TARGET_VOYAGE_MODEL", "voyage-4-large")
    target_dim: int = int(os.environ.get("VAQUILL_TARGET_DIM", "1024"))

    dry_run: bool = os.environ.get("VAQUILL_DRY_RUN", "0") == "1"


SETTINGS = Settings()
