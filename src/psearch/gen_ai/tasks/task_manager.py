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
Module to manage task status, preventing circular imports.
Based on the user-provided example.
"""
import datetime
import logging
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

# In a real app, this would be a database or a more robust in-memory store like Redis.
# For simplicity, we use a basic dictionary here.
# The keys will be task_id (e.g., UUID strings).
# Each value will be a dictionary holding task details.
task_status: Dict[str, Dict[str, Any]] = {}

def init_task(task_id: str, task_type: str, input_details: Optional[Dict[str, Any]] = None):
    """ 
    Initialize a new task entry.

    Args:
        task_id: Unique identifier for the task.
        task_type: Type of the task (e.g., "sql_generation", "pdf_processing").
        input_details: Optional dictionary of input parameters or metadata for the task.
    """
    if task_id in task_status:
        logger.warning(f"Task ID {task_id} already exists. Re-initializing.")
    
    task_status[task_id] = {
        "task_id": task_id,
        "task_type": task_type,
        "status": "received",
        "input_details": input_details or {},
        "result": None, # Can store final result, e.g., SQL script path or content
        "error": None,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "logs": []  # Initialize logs array
    }
    add_task_log(task_id, f"Task initialized. Type: {task_type}. Inputs: {input_details if input_details else 'N/A'}")
    logger.info(f"Task {task_id} initialized of type {task_type}.")

def update_task_status(
    task_id: str, 
    status: str, 
    result: Optional[Any] = None, # Changed from result_file to generic result
    error: Optional[str] = None
):
    """ 
    Safely update task status dictionary.

    Args:
        task_id: The ID of the task to update.
        status: The new status string (e.g., "processing", "completed", "failed").
        result: Optional. The result of the task if completed (e.g., generated SQL, file path).
        error: Optional. Error message if the task failed or encountered an error.
    """
    if task_id not in task_status:
        logger.error(f"Attempted to update non-existent task ID: {task_id}")
        # Optionally, initialize it here if that's desired behavior, or raise error
        # For now, just log and return to prevent crashes.
        # init_task(task_id, "unknown_type_on_update", {"note": "Implicitly initialized on update attempt"})
        return

    task_status[task_id]['status'] = status
    task_status[task_id]['updated_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    if result is not None: # Check for not None, as result could be an empty string or False
        task_status[task_id]['result'] = result
    
    if error:
        task_status[task_id]['error'] = error
        add_task_log(task_id, f"ERROR: {error}") # Log the error message
    else:
        # Clear previous error if status is not 'failed' and an error was present
        if status != 'failed' and 'error' in task_status[task_id] and task_status[task_id]['error'] is not None:
            task_status[task_id]['error'] = None # Set to None instead of deleting key
            add_task_log(task_id, "Previous error condition cleared.")
    
    log_message = f"Status changed to: {status}."
    if result is not None:
        log_message += f" Result updated." # Avoid logging potentially large result string
    add_task_log(task_id, log_message)
    logger.info(f"Task {task_id} status updated to {status}.")


def add_task_log(task_id: str, message: str):
    """ 
    Add a log entry to the task.

    Args:
        task_id: The ID of the task.
        message: The log message string.
    """
    if task_id not in task_status:
        # Log a warning but don't create a task here, as init_task should be called first.
        logger.warning(f"Attempted to add log to non-existent task ID: {task_id}. Log: '{message}'")
        return
    
    # Ensure logs array exists (should be created by init_task)
    if 'logs' not in task_status[task_id] or not isinstance(task_status[task_id]['logs'], list):
        task_status[task_id]['logs'] = []
        logger.warning(f"Re-initialized 'logs' array for task {task_id} as it was missing or not a list.")
    
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    task_status[task_id]['logs'].append({
        "timestamp": timestamp,
        "message": message
    })
    # logger.debug(f"Task {task_id} log added: {message}") # Can be noisy

def get_task_status(task_id: str) -> Optional[Dict[str, Any]]:
    """ 
    Get the status and details of a specific task. 
    
    Args:
        task_id: The ID of the task.
        
    Returns:
        A dictionary containing the task details, or None if the task_id is not found.
    """
    task_info = task_status.get(task_id)
    if task_info:
        logger.debug(f"Retrieved status for task {task_id}: {task_info.get('status')}")
        return task_info.copy() # Return a copy to prevent direct modification
    else:
        logger.warning(f"Task ID {task_id} not found in task_status store.")
        return None

def get_all_tasks_summary() -> List[Dict[str, Any]]:
    """
    Returns a summary of all tasks. Useful for admin or debugging.
    Each summary includes task_id, task_type, status, created_at, updated_at.
    """
    summary_list = []
    for task_id, details in task_status.items():
        summary_list.append({
            "task_id": task_id,
            "task_type": details.get("task_type"),
            "status": details.get("status"),
            "created_at": details.get("created_at"),
            "updated_at": details.get("updated_at"),
            "error": details.get("error") # Include error in summary
        })
    logger.info(f"Retrieved summary for {len(summary_list)} tasks.")
    return summary_list

# Example of how the user's example `update_task_status` parameters map:
# update_task_status(task_id: str, status: str, filename: str = None, result_file: str = None, error: str = None, gcs_uri: str = None):
# - filename: would be part of `input_details` in `init_task`
# - result_file: would be the `result` in `update_task_status`
# - gcs_uri: would be part of `input_details` in `init_task` or updated in `result` if it's an output.
# The current implementation is more generic. If specific fields like 'filename', 'gcs_uri' are always needed
# at the top level of the task status, `update_task_status` could be extended or they could be managed
# within the `result` dictionary or `input_details`. For now, `result` is generic.
