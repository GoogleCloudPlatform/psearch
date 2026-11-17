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
Task management module for handling background tasks and their statuses.
"""

from .task_manager import (
    MAX_STORED_TASKS,
    add_task_log,
    clear_tasks,
    get_all_tasks_summary,
    get_task_status,
    init_task,
    update_task_status,
)

__all__ = [
    "MAX_STORED_TASKS",
    "init_task",
    "update_task_status",
    "get_task_status",
    "add_task_log",
    "get_all_tasks_summary",
    "clear_tasks",
]
