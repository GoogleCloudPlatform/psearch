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

import json
import os
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class SchemaLoader:
    """Handles loading and providing access to the destination schema."""

    _destination_schema: Optional[Dict[str, Any]] = None
    _schema_path: Optional[str] = None

    @classmethod
    def get_destination_schema(cls) -> Optional[Dict[str, Any]]:
        """
        Loads the destination schema from schema.json if not already loaded.
        The schema file is expected to be in the same directory as the
        original SQLTransformationService or its refactored equivalent.
        """
        if cls._destination_schema is None:
            # Determine the path relative to this file's location,
            # assuming schema.json is in the parent 'services' directory.
            # services/sql/common/schema_utils.py -> services/schema.json
            current_dir = os.path.dirname(os.path.abspath(__file__))
            # Path to services/sql/common -> services/sql -> services -> schema.json
            cls._schema_path = os.path.join(current_dir, "..", "..", "schema.json")
            cls._schema_path = os.path.normpath(cls._schema_path) # Normalize path

            try:
                with open(cls._schema_path, 'r') as f:
                    cls._destination_schema = json.load(f)
                logger.info(f"Successfully loaded fixed destination schema from {cls._schema_path}")
            except FileNotFoundError:
                logger.error(f"Schema file not found at {cls._schema_path}. Ensure schema.json is correctly placed.")
                cls._destination_schema = None
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON from schema file {cls._schema_path}: {str(e)}")
                cls._destination_schema = None
            except Exception as e:
                logger.error(f"An unexpected error occurred while loading destination schema from {cls._schema_path}: {str(e)}")
                cls._destination_schema = None
        
        return cls._destination_schema

    @classmethod
    def get_schema_path(cls) -> Optional[str]:
        """Returns the path from which the schema was attempted to be loaded."""
        if cls._schema_path is None: # Ensure get_destination_schema has been called at least once
            cls.get_destination_schema()
        return cls._schema_path

# Example usage (optional, for testing or direct script run):
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    schema = SchemaLoader.get_destination_schema()
    if schema:
        logger.info(f"Schema loaded successfully. Keys: {list(schema.keys()) if isinstance(schema, dict) else 'Not a dict'}")
        logger.info(f"Schema path: {SchemaLoader.get_schema_path()}")
    else:
        logger.error("Failed to load schema.")
        logger.info(f"Attempted schema path: {SchemaLoader.get_schema_path()}")
