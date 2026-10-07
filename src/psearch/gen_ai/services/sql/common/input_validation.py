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

"""Input validation and output contract enforcement for the SQL generation pipeline.

This module is the single choke point that untrusted, caller-supplied values must
pass through before they are interpolated into an LLM prompt or into a BigQuery
query string.

Threat model
------------
The ``/generate-sql`` endpoint accepts table identifiers, column names, a
destination schema and a source data sample from the caller. Those values were
previously interpolated verbatim into:

  * the Gemini prompt built by ``InitialSQLGenerator._construct_prompt`` and
    ``SemanticEnhancer._construct_prompt`` (prompt injection / LLM proxying), and
  * the ``SELECT * FROM `{source_table_name}` `` sample query built by
    ``TransformationPipeline.execute_pipeline`` (SQL injection).

The defenses implemented here are layered:

  1. **Strict allow-listing of structured inputs.** Table IDs, column names and
     destination-schema field names/types only ever match a narrow character
     class. Newlines, backticks, quotes and prose cannot survive validation, so
     they can never reach the prompt or a query string.
  2. **Sanitisation + hard size caps for unavoidably free-form inputs.** The
     source data sample is real data, so it cannot be allow-listed; it is
     instead parsed as JSON, re-serialised, stripped of control characters and
     code-fence sequences, and truncated.
  3. **Output contract enforcement.** Whatever the model returns must be a
     single ``CREATE OR REPLACE TABLE`` statement that writes to the requested
     destination table and reads only from the requested source table. A model
     that has been successfully steered by an injected instruction fails this
     check and the task is failed instead of returned to the caller.

Nothing in this module requires Google Cloud credentials, so it is cheap to unit
test.
"""

import json
import logging
import re
from typing import Any, Dict, Iterable, List, Optional, Set

logger = logging.getLogger(__name__)


class InputValidationError(ValueError):
    """Raised when caller-supplied input fails validation.

    Callers at the HTTP boundary should translate this into a 4xx response;
    callers deeper in the pipeline should fail the task.
    """


class UnsafeSQLError(ValueError):
    """Raised when model-generated SQL violates the expected output contract."""


# --- Limits -----------------------------------------------------------------

MAX_SOURCE_SCHEMA_FIELDS = 2_000
MAX_CRITICAL_FIELDS = 200
MAX_SCHEMA_FIELDS_TOTAL = 2_000
MAX_SCHEMA_DEPTH = 15
MAX_DESCRIPTION_LENGTH = 200
MAX_DATA_SAMPLE_CHARS = 20_000
MAX_DATA_SAMPLE_ROWS = 10
MAX_GENERATED_SQL_CHARS = 200_000

# --- Identifier grammars ----------------------------------------------------
# Deliberately narrower than what BigQuery itself accepts. Quoted identifiers
# containing spaces/backticks/newlines are rejected outright: supporting them
# would re-open the injection channel this module exists to close.

_PROJECT_RE = r"[A-Za-z0-9][A-Za-z0-9\-]{4,28}[A-Za-z0-9]"
_DATASET_RE = r"[A-Za-z0-9_]{1,1024}"
_TABLE_RE = r"[A-Za-z0-9_]{1,1024}"

TABLE_ID_RE = re.compile(
    rf"^(?:(?P<project>{_PROJECT_RE})\.)?(?P<dataset>{_DATASET_RE})\.(?P<table>{_TABLE_RE})$"
)

COLUMN_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,299}$")

# A dotted path into a (possibly nested) column, e.g. "priceInfo.price".
FIELD_PATH_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,299}(?:\.[A-Za-z_][A-Za-z0-9_]{0,299}){0,9}$")

ALLOWED_SCHEMA_TYPES: Set[str] = {
    "STRING", "BYTES", "INTEGER", "INT64", "SMALLINT", "BIGINT", "TINYINT",
    "FLOAT", "FLOAT64", "NUMERIC", "DECIMAL", "BIGNUMERIC", "BIGDECIMAL",
    "BOOLEAN", "BOOL", "TIMESTAMP", "DATE", "TIME", "DATETIME", "INTERVAL",
    "GEOGRAPHY", "JSON", "RECORD", "STRUCT",
}
_NESTED_TYPES = {"RECORD", "STRUCT"}
ALLOWED_SCHEMA_MODES: Set[str] = {"NULLABLE", "REQUIRED", "REPEATED"}
_ALLOWED_SCHEMA_KEYS = {"name", "type", "mode", "fields", "description"}

# Characters that would let a value break out of the prompt scaffolding
# (code fences, control characters, bidi overrides).
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f\u202a-\u202e\u2066-\u2069]")
_BACKTICK_RUN_RE = re.compile(r"`{2,}")


# --- Structured input validation -------------------------------------------


def validate_table_id(value: Any, field_name: str = "table") -> str:
    """Validate a BigQuery table identifier of the form ``[project.]dataset.table``.

    Returns the validated identifier unchanged so it is safe to interpolate into
    a backtick-quoted BigQuery reference and into an LLM prompt.

    Raises:
        InputValidationError: if the value is not a well-formed identifier.
    """
    if not isinstance(value, str):
        raise InputValidationError(f"{field_name} must be a string, got {type(value).__name__}.")

    candidate = value.strip()
    if not candidate:
        raise InputValidationError(f"{field_name} must not be empty.")
    if len(candidate) > 2_200:
        raise InputValidationError(f"{field_name} is too long.")
    if not TABLE_ID_RE.match(candidate):
        raise InputValidationError(
            f"{field_name} must be a BigQuery table ID of the form "
            f"'project.dataset.table' or 'dataset.table' using only letters, "
            f"digits, underscores and hyphens. Received: {candidate[:80]!r}"
        )
    return candidate


def split_table_id(table_id: str, default_project: Optional[str] = None) -> Dict[str, str]:
    """Split an already-validated table ID into its project/dataset/table parts."""
    match = TABLE_ID_RE.match(table_id)
    if not match:
        raise InputValidationError(f"Not a valid table ID: {table_id[:80]!r}")
    project = match.group("project") or default_project
    if not project:
        raise InputValidationError(
            f"Table ID {table_id!r} has no project and no default project was supplied."
        )
    return {
        "project": project,
        "dataset": match.group("dataset"),
        "table": match.group("table"),
    }


def validate_column_name(value: Any, field_name: str = "column") -> str:
    """Validate a single (non-nested) BigQuery column name."""
    if not isinstance(value, str):
        raise InputValidationError(f"{field_name} must be a string, got {type(value).__name__}.")
    candidate = value.strip()
    if not COLUMN_NAME_RE.match(candidate):
        raise InputValidationError(
            f"{field_name} must start with a letter or underscore and contain only "
            f"letters, digits and underscores (max 300 chars). Received: {candidate[:80]!r}"
        )
    return candidate


def validate_field_path(value: Any, field_name: str = "field") -> str:
    """Validate a possibly nested field reference such as ``priceInfo.price``."""
    if not isinstance(value, str):
        raise InputValidationError(f"{field_name} must be a string, got {type(value).__name__}.")
    candidate = value.strip()
    if not FIELD_PATH_RE.match(candidate):
        raise InputValidationError(
            f"{field_name} must be a dot-separated field path using only letters, "
            f"digits and underscores. Received: {candidate[:80]!r}"
        )
    return candidate


def validate_source_schema_fields(values: Any) -> List[str]:
    """Validate the list of source column names supplied by the caller."""
    if not isinstance(values, (list, tuple)):
        raise InputValidationError("source_schema_fields must be a list of column names.")
    if not values:
        raise InputValidationError("source_schema_fields must not be empty.")
    if len(values) > MAX_SOURCE_SCHEMA_FIELDS:
        raise InputValidationError(
            f"source_schema_fields must contain at most {MAX_SOURCE_SCHEMA_FIELDS} entries."
        )
    return [validate_column_name(v, f"source_schema_fields[{i}]") for i, v in enumerate(values)]


def validate_critical_fields(values: Any) -> List[str]:
    """Validate the optional list of critical destination fields to refine."""
    if values is None:
        return []
    if not isinstance(values, (list, tuple)):
        raise InputValidationError("critical_fields_to_refine must be a list of field paths.")
    if len(values) > MAX_CRITICAL_FIELDS:
        raise InputValidationError(
            f"critical_fields_to_refine must contain at most {MAX_CRITICAL_FIELDS} entries."
        )
    return [validate_field_path(v, f"critical_fields_to_refine[{i}]") for i, v in enumerate(values)]


def sanitize_description(value: Any) -> str:
    """Make a free-text schema description safe to embed in a prompt.

    Descriptions are documentation, not instructions: control characters and
    code-fence sequences are removed and the text is truncated so it cannot
    carry a meaningful injected payload.
    """
    text = value if isinstance(value, str) else str(value)
    text = _CONTROL_CHARS_RE.sub(" ", text)
    text = text.replace("\r", " ").replace("\n", " ")
    text = _BACKTICK_RUN_RE.sub("'", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > MAX_DESCRIPTION_LENGTH:
        text = text[:MAX_DESCRIPTION_LENGTH] + "…"
    return text


def validate_destination_schema(schema: Any) -> Any:
    """Validate and rebuild a destination schema from allow-listed parts only.

    Both shapes used in this codebase are accepted: a bare list of field
    definitions (as stored in ``services/schema.json``) and a
    ``{"fields": [...]}`` wrapper. The caller's container type is preserved.

    The returned schema is a *new* object containing only known keys with
    validated names/types, so any extra keys or prose an attacker embedded in
    the submitted JSON are dropped before the schema is serialised into a
    prompt.

    Raises:
        InputValidationError: if the schema is malformed or exceeds the limits.
    """
    if isinstance(schema, list):
        fields: Any = schema
        wrap_in_dict = False
    elif isinstance(schema, dict):
        fields = schema.get("fields")
        wrap_in_dict = True
    else:
        raise InputValidationError(
            "destination_schema must be a JSON array of fields or an object with a 'fields' list."
        )

    if not isinstance(fields, list) or not fields:
        raise InputValidationError("destination_schema must contain a non-empty 'fields' list.")

    counter = {"n": 0}
    validated_fields = _validate_schema_fields(fields, depth=0, counter=counter, path="fields")
    return {"fields": validated_fields} if wrap_in_dict else validated_fields


def _validate_schema_fields(
    fields: Iterable[Any], depth: int, counter: Dict[str, int], path: str
) -> List[Dict[str, Any]]:
    if depth > MAX_SCHEMA_DEPTH:
        raise InputValidationError(
            f"destination_schema nests deeper than the maximum of {MAX_SCHEMA_DEPTH} levels."
        )

    validated: List[Dict[str, Any]] = []
    for index, field in enumerate(fields):
        field_path = f"{path}[{index}]"
        counter["n"] += 1
        if counter["n"] > MAX_SCHEMA_FIELDS_TOTAL:
            raise InputValidationError(
                f"destination_schema contains more than {MAX_SCHEMA_FIELDS_TOTAL} fields."
            )
        if not isinstance(field, dict):
            raise InputValidationError(f"{field_path} must be a JSON object.")

        unknown_keys = set(field) - _ALLOWED_SCHEMA_KEYS
        if unknown_keys:
            raise InputValidationError(
                f"{field_path} contains unsupported keys: {sorted(unknown_keys)}. "
                f"Allowed keys: {sorted(_ALLOWED_SCHEMA_KEYS)}."
            )

        name = validate_column_name(field.get("name"), f"{field_path}.name")

        raw_type = field.get("type")
        if raw_type is None:
            field_type = "STRING"
        elif not isinstance(raw_type, str):
            raise InputValidationError(f"{field_path}.type must be a string.")
        else:
            # An empty string means "not specified" in the committed schema.json.
            field_type = raw_type.strip().upper() or "STRING"
        if field_type not in ALLOWED_SCHEMA_TYPES:
            raise InputValidationError(
                f"{field_path}.type {raw_type[:40]!r} is not a supported BigQuery type. "
                f"Allowed types: {sorted(ALLOWED_SCHEMA_TYPES)}."
            )

        validated_field: Dict[str, Any] = {"name": name, "type": field_type}

        raw_mode = field.get("mode")
        if raw_mode is not None:
            if not isinstance(raw_mode, str):
                raise InputValidationError(f"{field_path}.mode must be a string.")
            mode = raw_mode.strip().upper()
            if mode and mode not in ALLOWED_SCHEMA_MODES:
                raise InputValidationError(
                    f"{field_path}.mode {raw_mode[:40]!r} must be one of "
                    f"{sorted(ALLOWED_SCHEMA_MODES)}."
                )
            if mode:
                validated_field["mode"] = mode

        if field.get("description") is not None:
            description = sanitize_description(field["description"])
            if description:
                validated_field["description"] = description

        nested = field.get("fields")
        if field_type in _NESTED_TYPES:
            if not isinstance(nested, list) or not nested:
                raise InputValidationError(
                    f"{field_path} is of type {field_type} and must declare a non-empty "
                    f"'fields' list."
                )
            validated_field["fields"] = _validate_schema_fields(
                nested, depth=depth + 1, counter=counter, path=f"{field_path}.fields"
            )
        elif nested:
            # Scalar fields in schema.json carry an empty "fields": [] placeholder,
            # which is dropped silently; a populated list on a scalar type is an error.
            raise InputValidationError(
                f"{field_path} declares nested 'fields' but its type is {field_type}."
            )

        validated.append(validated_field)

    return validated


# --- Free-form input sanitisation ------------------------------------------


def sanitize_data_sample_json(value: Any) -> Optional[str]:
    """Normalise the caller-supplied source data sample before it enters a prompt.

    The sample is genuine data and therefore cannot be allow-listed, so it is
    defanged instead: it must parse as JSON, it is re-serialised (dropping any
    surrounding prose), control characters and code-fence sequences are removed,
    and both row count and total length are capped.

    Returns ``None`` when no usable sample was supplied.

    Raises:
        InputValidationError: if the value is not valid JSON.
    """
    if value is None:
        return None

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if len(text) > MAX_DATA_SAMPLE_CHARS * 4:
            raise InputValidationError(
                f"source_data_sample_json exceeds the maximum size of "
                f"{MAX_DATA_SAMPLE_CHARS * 4} characters."
            )
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise InputValidationError(
                f"source_data_sample_json must be a valid JSON document: {exc}"
            ) from exc
    else:
        parsed = value

    if isinstance(parsed, dict):
        parsed = [parsed]
    if not isinstance(parsed, list):
        raise InputValidationError(
            "source_data_sample_json must be a JSON array of row objects."
        )

    rows = parsed[:MAX_DATA_SAMPLE_ROWS]
    if not rows:
        return None

    try:
        serialized = json.dumps(rows, default=str, ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        raise InputValidationError(f"source_data_sample_json is not serialisable: {exc}") from exc

    serialized = _CONTROL_CHARS_RE.sub(" ", serialized)
    serialized = _BACKTICK_RUN_RE.sub("'", serialized)
    if len(serialized) > MAX_DATA_SAMPLE_CHARS:
        serialized = serialized[:MAX_DATA_SAMPLE_CHARS] + " …/* truncated */"
        logger.info("Source data sample truncated to %d characters.", MAX_DATA_SAMPLE_CHARS)
    return serialized


# --- Output contract enforcement -------------------------------------------

_SQL_NOISE_RE = re.compile(
    r"'''.*?'''|"
    r'""".*?"""|'
    r"'(?:\\.|[^'\\])*'|"
    r'"(?:\\.|[^"\\])*"|'
    r"/\*.*?\*/|"
    r"(?:--|#)[^\n]*",
    re.DOTALL,
)
_IDENT_SEGMENT = r"(?:`[^`]+`|[A-Za-z0-9_\-]+)"
_BACKTICKED_RE = re.compile(rf"(?:{_IDENT_SEGMENT}\s*\.\s*)*`[^`]+`(?:\s*\.\s*{_IDENT_SEGMENT})*")
_FUNCTION_PARENS_RE = re.compile(
    r"\b(EXTRACT|TRIM)\s*\((?!\s*(?:SELECT|WITH)\b)(?:(?!\bJOIN\b)[^()])*\)"
    r"|\b(?!(?:FROM|JOIN|ON|USING|WHERE|AND|OR|AS|SELECT|WITH|TABLE|INTO|UPDATE|DELETE|INSERT|MERGE)\b)"
    r"([A-Za-z0-9_\-]+)\s*\((?!\s*(?:SELECT|WITH)\b)(?:(?!\b(?:FROM|JOIN)\b)[^()])*\)",
    re.IGNORECASE | re.DOTALL,
)
_GROUPING_PARENS_RE = re.compile(
    r"\((?!\s*(?:SELECT|WITH)\b)([^()]*)\)", re.IGNORECASE | re.DOTALL
)
_FROM_JOIN_BLOCK_RE = re.compile(
    r"\b(?:FROM|JOIN)(?:\s+|(?=`))([^;()]+?)"
    r"(?=\b(?:WHERE|GROUP|ORDER|LIMIT|OFFSET|HAVING|WINDOW|QUALIFY|UNION|EXCEPT|INTERSECT|ON|USING|LEFT|RIGHT|INNER|CROSS|OUTER|FULL|JOIN|SELECT|FROM)\b|;|[()]|$)",
    re.IGNORECASE | re.DOTALL,
)
_TABLE_REF_PREFIX_RE = re.compile(
    rf"^({_IDENT_SEGMENT}(?:\s*\.\s*{_IDENT_SEGMENT})*)"
)

# Statements/constructs that a schema-mapping script never needs. Matched
# against SQL with comments and string literals removed.
_FORBIDDEN_CONSTRUCTS = [
    (re.compile(r"\bEXECUTE\s+IMMEDIATE\b", re.IGNORECASE), "EXECUTE IMMEDIATE (dynamic SQL)"),
    (re.compile(r"\bEXPORT\s+DATA\b", re.IGNORECASE), "EXPORT DATA"),
    (re.compile(r"\bLOAD\s+DATA\b", re.IGNORECASE), "LOAD DATA"),
    (re.compile(r"(?<!\.)(?<!\.`)\bDROP(?:\s+|(?=[`\(]))[`\w(]", re.IGNORECASE), "DROP"),
    (re.compile(r"\bTRUNCATE\s+TABLE\b", re.IGNORECASE), "TRUNCATE TABLE"),
    (re.compile(r"(?<!\.)(?<!\.`)\bDELETE(?:\s+FROM)?(?:\s+|(?=[`\(]))[`\w(]", re.IGNORECASE), "DELETE"),
    (re.compile(r"(?<!\.)(?<!\.`)\bINSERT(?:\s+INTO)?(?:\s+|(?=[`\(]))[`\w(]", re.IGNORECASE), "INSERT"),
    (re.compile(r"(?<!\.)(?<!\.`)\bUPDATE(?:\s+|(?=[`\(]))[`\w(]", re.IGNORECASE), "UPDATE"),
    (re.compile(r"(?<!\.)(?<!\.`)\bMERGE(?:\s+INTO)?(?:\s+|(?=[`\(]))[`\w(]", re.IGNORECASE), "MERGE"),
    (re.compile(r"\bALTER\s+(?:TABLE|SCHEMA|VIEW|MODEL|ORGANIZATION|PROJECT)\b", re.IGNORECASE), "ALTER"),
    (re.compile(r"(?<!\.)(?<!\.`)\b(?:GRANT|REVOKE)(?:\s+|(?=[`\(]))[`\w(]", re.IGNORECASE), "GRANT/REVOKE"),
    (re.compile(r"(?<!\.)(?<!\.`)\bCALL(?:\s+|(?=[`\(]))[`\w(]", re.IGNORECASE), "CALL"),
    # Any CREATE after the single expected header is unexpected.
    (re.compile(r"(?<!\.)(?<!\.`)\bCREATE(?:\s+OR\s+REPLACE)?(?:\s+|(?=[`\(]))[`\w(]", re.IGNORECASE), "additional CREATE statement"),

    (re.compile(r"\bEXTERNAL_QUERY\s*\(", re.IGNORECASE), "EXTERNAL_QUERY"),
    (re.compile(r"\bSET\s+@@", re.IGNORECASE), "system variable assignment"),
    (re.compile(r"\bBEGIN\b|\bDECLARE\b", re.IGNORECASE), "scripting block"),
]


def _strip_sql_noise(sql: str) -> str:
    """Remove comments and string literals so keyword scanning cannot be fooled."""
    def _replace(match: re.Match[str]) -> str:
        text = match.group(0)
        if text.startswith(("--", "#", "/*")):
            return " "
        return "''"

    return _SQL_NOISE_RE.sub(_replace, sql)


def enforce_sql_contract(
    sql_query: Optional[str],
    destination_table_name: str,
    source_table_name: str,
) -> str:
    """Verify model-generated SQL still matches the contract the caller asked for.

    This is the backstop against a successful prompt injection: even if the model
    is persuaded to emit something else, it never reaches the caller or a
    BigQuery job unless it is a single ``CREATE OR REPLACE TABLE`` statement
    that writes to ``destination_table_name`` and reads only from
    ``source_table_name``.

    Returns:
        The SQL, unchanged, when it satisfies the contract.

    Raises:
        UnsafeSQLError: when the SQL violates the contract.
    """
    if not sql_query or not sql_query.strip():
        raise UnsafeSQLError("Generated SQL is empty.")
    if len(sql_query) > MAX_GENERATED_SQL_CHARS:
        raise UnsafeSQLError(
            f"Generated SQL exceeds the maximum length of {MAX_GENERATED_SQL_CHARS} characters."
        )

    destination = validate_table_id(destination_table_name, "destination_table")
    source = validate_table_id(source_table_name, "source_table")
    allowed_tables = {destination.lower(), source.lower()}

    stripped = _strip_sql_noise(sql_query).strip()

    statements = [s for s in stripped.split(";") if s.strip()]
    if len(statements) > 1:
        raise UnsafeSQLError(
            f"Generated SQL must be a single statement, found {len(statements)}."
        )

    header = re.match(
        r"^\s*CREATE\s+OR\s+REPLACE\s+TABLE\s+`?([A-Za-z0-9_.\-]+)`?\s+(?:OPTIONS\s*\(.*?\)\s*)?AS\s+",
        stripped,
        re.IGNORECASE | re.DOTALL,
    )
    if not header:
        raise UnsafeSQLError(
            "Generated SQL must start with "
            "'CREATE OR REPLACE TABLE `<destination>` AS SELECT ...'. "
            f"Got: {stripped[:120]!r}"
        )

    target = header.group(1)
    if target.lower() != destination.lower():
        raise UnsafeSQLError(
            f"Generated SQL writes to {target!r} but the requested destination table is "
            f"{destination!r}."
        )

    for pattern, label in _FORBIDDEN_CONSTRUCTS:
        match = pattern.search(stripped[header.end():])
        if match:
            raise UnsafeSQLError(
                f"Generated SQL contains a disallowed construct ({label}): {match.group(0)!r}."
            )

    collapsed = stripped
    while True:
        while True:
            collapsed, fn_count = _FUNCTION_PARENS_RE.subn(
                lambda m: m.group(1) or m.group(2), collapsed
            )
            collapsed, grp_count = _GROUPING_PARENS_RE.subn(r" \1 ", collapsed)
            if fn_count == 0 and grp_count == 0:
                break

        for block in _FROM_JOIN_BLOCK_RE.findall(collapsed):
            for part in block.split(","):
                part = part.strip()
                if not part:
                    continue
                ref_match = _TABLE_REF_PREFIX_RE.match(part)
                if not ref_match:
                    continue
                ref = re.sub(r"[\s`]+", "", ref_match.group(1))
                if "." not in ref:
                    # A CTE name, a table alias or an UNNEST/subquery target: harmless.
                    continue
                if ref.lower() not in allowed_tables:
                    raise UnsafeSQLError(
                        f"Generated SQL reads from unexpected table {ref!r}. Only "
                        f"{sorted(allowed_tables)} are allowed."
                    )

        collapsed, count = re.subn(r"\([^()]*\)", " subquery ", collapsed)
        if count == 0:
            break

    for quoted in _BACKTICKED_RE.findall(stripped):
        candidate = re.sub(r"[\s`]+", "", quoted)
        if "." not in candidate:
            continue
        if candidate.lower() not in allowed_tables:
            raise UnsafeSQLError(
                f"Generated SQL references unexpected qualified identifier {candidate!r}."
            )

    return sql_query
