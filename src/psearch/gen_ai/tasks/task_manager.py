#
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Thread-safe, bounded in-memory task status store, preventing circular imports.
"""

import copy
import datetime
import logging
import threading
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

MAX_STORED_TASKS = 1000
_TERMINAL_STATUSES = {
    "completed",
    "failed",
    "failed_validation_final_sql_available",
}

_lock = threading.Lock()

# In a real app, this would be a database or a distributed store like Redis.
# Keys are task_id strings; values hold task status details.
task_status: Dict[str, Dict[str, Any]] = {}


def clear_tasks() -> None:
    """Clear all stored tasks in a thread-safe manner."""
    with _lock:
        task_status.clear()


def _add_task_log_unlocked(task_id: str, message: str) -> None:
    """Append a log entry to an existing task while ``_lock`` is already held."""
    if task_id not in task_status:
        logger.warning(
            f"Attempted to add log to non-existent task ID: {task_id}. Log: '{message}'"
        )
        return

    if "logs" not in task_status[task_id] or not isinstance(
        task_status[task_id]["logs"], list
    ):
        task_status[task_id]["logs"] = []
        logger.warning(
            f"Re-initialized 'logs' array for task {task_id} as it was missing or not a list."
        )

    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    task_status[task_id]["logs"].append(
        {
            "timestamp": timestamp,
            "message": message,
        }
    )


def _evict_if_at_capacity_unlocked() -> None:
    """Evict oldest terminal task(s) (falling back to oldest overall) when at cap."""
    while len(task_status) >= MAX_STORED_TASKS and task_status:
        evict_key: Optional[str] = None
        for existing_id, details in task_status.items():
            if details.get("status") in _TERMINAL_STATUSES:
                evict_key = existing_id
                break
        if evict_key is None:
            evict_key = next(iter(task_status))
        del task_status[evict_key]


def init_task(
    task_id: str, task_type: str, input_details: Optional[Dict[str, Any]] = None
) -> None:
    """Initialize a new task entry, evicting oldest terminal entries when at capacity."""
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with _lock:
        if task_id in task_status:
            logger.warning(f"Task ID {task_id} already exists. Re-initializing.")
            del task_status[task_id]
        else:
            _evict_if_at_capacity_unlocked()

        task_status[task_id] = {
            "task_id": task_id,
            "task_type": task_type,
            "status": "received",
            "input_details": copy.deepcopy(input_details) if input_details else {},
            "result": None,
            "error": None,
            "created_at": now_iso,
            "updated_at": now_iso,
            "logs": [],
        }
        _add_task_log_unlocked(
            task_id,
            f"Task initialized. Type: {task_type}. Inputs: {input_details if input_details else 'N/A'}",
        )
    logger.info(f"Task {task_id} initialized of type {task_type}.")


def update_task_status(
    task_id: str,
    status: str,
    result: Optional[Any] = None,
    error: Optional[str] = None,
) -> None:
    """Safely update task status dictionary under lock."""
    with _lock:
        if task_id not in task_status:
            logger.error(f"Attempted to update non-existent task ID: {task_id}")
            return

        task_status[task_id]["status"] = status
        task_status[task_id]["updated_at"] = datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat()

        if result is not None:
            task_status[task_id]["result"] = result

        if error:
            task_status[task_id]["error"] = error
            _add_task_log_unlocked(task_id, f"ERROR: {error}")
        elif (
            status != "failed"
            and "error" in task_status[task_id]
            and task_status[task_id]["error"] is not None
        ):
            task_status[task_id]["error"] = None
            _add_task_log_unlocked(task_id, "Previous error condition cleared.")

        log_message = f"Status changed to: {status}."
        if result is not None:
            log_message += " Result updated."
        _add_task_log_unlocked(task_id, log_message)

    logger.info(f"Task {task_id} status updated to {status}.")


def add_task_log(task_id: str, message: str) -> None:
    """Add a log entry to the task in a thread-safe manner."""
    with _lock:
        _add_task_log_unlocked(task_id, message)


def get_task_status(task_id: str) -> Optional[Dict[str, Any]]:
    """Return an isolated copy of a task's status details, or ``None`` if not found."""
    with _lock:
        task_info = task_status.get(task_id)
        if task_info is None:
            snapshot = None
        else:
            snapshot = copy.deepcopy(task_info)

    if snapshot is not None:
        logger.debug(
            f"Retrieved status for task {task_id}: {snapshot.get('status')}"
        )
        return snapshot

    logger.warning(f"Task ID {task_id} not found in task_status store.")
    return None


def get_all_tasks_summary() -> List[Dict[str, Any]]:
    """Return a thread-safe summary list of all stored tasks."""
    with _lock:
        summary_list = [
            {
                "task_id": task_id,
                "task_type": details.get("task_type"),
                "status": details.get("status"),
                "created_at": details.get("created_at"),
                "updated_at": details.get("updated_at"),
                "error": details.get("error"),
            }
            for task_id, details in task_status.items()
        ]
    logger.info(f"Retrieved summary for {len(summary_list)} tasks.")
    return summary_list
