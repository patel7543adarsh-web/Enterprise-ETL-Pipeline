"""
State Store for Incremental Extraction Tracking.
Persists high-watermark timestamps, cursors, and run statuses.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class IncrementalStateStore:
    """
    Tracks and persists the extraction state (high watermark datetime, last cursor)
    per (source, entity) pair to ensure idempotent incremental ETL loads.
    """

    def __init__(self, state_file_path: str = "./data_lake_staging/state.json"):
        self.state_file = Path(state_file_path)
        self.state: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _state_key(self, source: str, entity: str) -> str:
        return f"{source.lower()}::{entity.lower()}"

    def _load(self) -> None:
        """Loads state from JSON file if present."""
        if self.state_file.exists():
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    self.state = json.load(f)
            except Exception as e:
                logger.warning(f"Could not load state file ({e}). Starting with clean state.")
                self.state = {}
        else:
            self.state = {}

    def _save(self) -> None:
        """Persists state to disk."""
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2, default=str)

    def get_watermark(self, source: str, entity: str) -> Optional[datetime]:
        """Gets last extracted timestamp for given source and entity."""
        key = self._state_key(source, entity)
        entry = self.state.get(key, {})
        wm_str = entry.get("last_extracted_timestamp")
        if wm_str:
            try:
                return datetime.fromisoformat(wm_str)
            except ValueError:
                return None
        return None

    def get_cursor(self, source: str, entity: str) -> Optional[str]:
        """Gets last pagination cursor for given source and entity."""
        key = self._state_key(source, entity)
        return self.state.get(key, {}).get("last_cursor")

    def update_state(
        self,
        source: str,
        entity: str,
        last_extracted_timestamp: Optional[datetime] = None,
        last_cursor: Optional[str] = None,
        records_synced: int = 0,
        status: str = "COMPLETED",
    ) -> None:
        """Updates and persists the high watermark state."""
        key = self._state_key(source, entity)
        now = datetime.now(timezone.utc)
        self.state[key] = {
            "source": source.lower(),
            "entity": entity.lower(),
            "last_extracted_timestamp": (
                last_extracted_timestamp.isoformat()
                if last_extracted_timestamp
                else self.state.get(key, {}).get("last_extracted_timestamp")
            ),
            "last_cursor": last_cursor,
            "last_run_at": now.isoformat(),
            "records_synced_last_run": records_synced,
            "status": status,
        }
        self._save()
        logger.info(f"Updated state for {key}: watermark={last_extracted_timestamp}, cursor={last_cursor}")

    def reset_state(self, source: Optional[str] = None, entity: Optional[str] = None) -> None:
        """Clears state for source/entity or all."""
        if source and entity:
            key = self._state_key(source, entity)
            self.state.pop(key, None)
        elif source:
            keys_to_del = [k for k in self.state if k.startswith(f"{source.lower()}::")]
            for k in keys_to_del:
                del self.state[k]
        else:
            self.state = {}
        self._save()
