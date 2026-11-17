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
import logging
import os
import re  # For parsing error messages
from typing import Any, Dict, Optional

from google.api_core.exceptions import BadRequest, Forbidden, NotFound
from google.cloud import bigquery

logger = logging.getLogger(__name__)

_MASKED_IDENT_SEG = r"(?:`_+`|[A-Za-z0-9_\-]+)"
_CTAS_HEADER_RE = re.compile(
    rf"^\s*CREATE(?:\s+OR\s+REPLACE)?(?:\s+(?:TEMP|TEMPORARY))?\s+TABLE"
    rf"(?:\s+IF\s+NOT\s+EXISTS)?\s+"
    rf"(?P<table_ident>{_MASKED_IDENT_SEG}(?:\s*\.\s*{_MASKED_IDENT_SEG})*)"
    rf"(?:(?<=`)|(?=\s|\())",
    re.IGNORECASE,
)
_CTAS_PRE_AS_CLAUSES_RE = re.compile(
    r"^(?:\(|DEFAULT\s+COLLATE\b|PARTITION\s+BY\b|CLUSTER\s+BY\b|OPTIONS\b)",
    re.IGNORECASE,
)
_CTAS_BODY_START_RE = re.compile(
    r"^(?:SELECT\b|WITH\b|\(\s*(?:\(\s*)*(?:SELECT\b|WITH\b))",
    re.IGNORECASE,
)


def _sanitize_bq_error_message(
    raw: str, fallback: str = "SQL validation failed."
) -> str:
    """Strip REST transport prefixes and URLs from BigQuery SDK exception strings."""
    cleaned = raw.strip()
    cleaned = re.sub(
        r"^(?:\d{3}\s+)?(?:GET|POST|PUT|DELETE|PATCH)\s+https?://\S+?:\s*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"^\d{3}\s+", "", cleaned)
    cleaned = re.sub(r"https?://\S+", "[redacted-url]", cleaned)
    cleaned = re.sub(
        r"\b[a-zA-Z][a-zA-Z0-9+.-]*://\S+",
        "[redacted-url]",
        cleaned,
    )
    return cleaned.strip() or fallback


def _mask_sql_preserving_length(sql: str) -> Optional[str]:
    """Mask SQL comments, string literals, and backticked identifiers without changing length.

    Returns ``None`` if any block comment, quoted literal, or backticked identifier is unclosed.
    """
    out: list[str] = []
    n = len(sql)
    i = 0
    while i < n:
        ch = sql[i]
        if (ch == "-" and i + 1 < n and sql[i + 1] == "-") or ch == "#":
            while i < n and sql[i] != "\n":
                out.append(" ")
                i += 1
            continue

        if ch == "/" and i + 1 < n and sql[i + 1] == "*":
            out.append("  ")
            i += 2
            closed = False
            while i < n:
                if sql[i] == "*" and i + 1 < n and sql[i + 1] == "/":
                    out.append("  ")
                    i += 2
                    closed = True
                    break
                out.append("\n" if sql[i] == "\n" else " ")
                i += 1
            if not closed:
                return None
            continue

        if ch in ("'", '"') and i + 2 < n and sql[i : i + 3] == ch * 3:
            delim = ch * 3
            out.append(ch + "  ")
            i += 3
            closed = False
            while i < n:
                if sql[i : i + 3] == delim:
                    out.append("  " + ch)
                    i += 3
                    closed = True
                    break
                if sql[i] == "\\" and i + 1 < n:
                    out.append(" ")
                    out.append("\n" if sql[i + 1] == "\n" else " ")
                    i += 2
                    continue
                out.append("\n" if sql[i] == "\n" else " ")
                i += 1
            if not closed:
                return None
            continue

        if ch in ("'", '"'):
            quote = ch
            out.append(quote)
            i += 1
            closed = False
            while i < n:
                if sql[i] == quote:
                    out.append(quote)
                    i += 1
                    closed = True
                    break
                if sql[i] == "\\" and i + 1 < n:
                    out.append(" ")
                    out.append("\n" if sql[i + 1] == "\n" else " ")
                    i += 2
                    continue
                if sql[i] == "\n":
                    return None
                out.append(" ")
                i += 1
            if not closed:
                return None
            continue

        if ch == "`":
            out.append("`")
            i += 1
            closed = False
            while i < n:
                if sql[i] == "`":
                    out.append("`")
                    i += 1
                    closed = True
                    break
                if sql[i] == "\\" and i + 1 < n:
                    out.append("_")
                    out.append("\n" if sql[i + 1] == "\n" else "_")
                    i += 2
                    continue
                out.append("\n" if sql[i] == "\n" else "_")
                i += 1
            if not closed:
                return None
            continue

        out.append(ch)
        i += 1

    masked = "".join(out)
    assert len(masked) == n
    return masked


def _extract_ctas_query_body(sql_script: str) -> Optional[str]:
    """Extract ``<query_body>`` from a single ``CREATE ... TABLE ... AS <query_body>[;]`` statement.

    Uses length-preserving comment/literal/backtick masking and parenthesis-depth tracking so
    ``AS`` or ``;`` inside ``OPTIONS(...)``, ``PARTITION BY CAST(... AS ...)``, strings, or
    comments can never misalign offsets or smuggle multiple statements.
    """
    masked = _mask_sql_preserving_length(sql_script)
    if masked is None:
        return None

    header_match = _CTAS_HEADER_RE.match(masked)
    if not header_match:
        return None

    depth = 0
    as_end: Optional[int] = None
    pos = header_match.end()
    n = len(masked)
    i = pos
    while i < n:
        ch = masked[i]
        if ch == ";":
            return None
        if ch == "(":
            depth += 1
            i += 1
            continue
        if ch == ")":
            depth -= 1
            if depth < 0:
                return None
            i += 1
            continue
        if depth == 0 and masked[i : i + 2].upper() == "AS":
            prev_is_ident = i > 0 and (masked[i - 1].isalnum() or masked[i - 1] == "_")
            next_is_ident = (i + 2 < n) and (
                masked[i + 2].isalnum() or masked[i + 2] == "_"
            )
            if not prev_is_ident and not next_is_ident:
                between = masked[pos:i].strip()
                if between and not _CTAS_PRE_AS_CLAUSES_RE.match(between):
                    return None
                as_end = i + 2
                break
        i += 1

    if as_end is None:
        return None

    masked_body = masked[as_end:]
    raw_body = sql_script[as_end:]

    masked_rstripped = masked_body.rstrip()
    if not masked_rstripped:
        return None

    if masked_rstripped.endswith(";"):
        core_end = len(masked_rstripped) - 1
    else:
        core_end = len(masked_rstripped)

    masked_core = masked_body[:core_end]
    if ";" in masked_core:
        return None

    masked_lstripped = masked_core.lstrip()
    if not masked_lstripped or not _CTAS_BODY_START_RE.match(masked_lstripped):
        return None

    lead_trim = len(masked_core) - len(masked_lstripped)
    end_trim = len(masked_core.rstrip())
    extracted = raw_body[lead_trim:end_trim]
    return extracted if extracted.strip() else None


class SQLValidator:
    """
    Validates SQL queries using BigQuery dry runs.
    Corresponds to Step 4 of the multi-step SQL strategy.
    """

    def __init__(self, project_id: str):
        """
        Initializes the SQLValidator.

        Args:
            project_id: The Google Cloud Project ID.
        """
        self.project_id = project_id
        try:
            self.bigquery_client = bigquery.Client(project=project_id)
            logger.info(f"BigQuery client initialized for project: {project_id}")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client for project {project_id}: {str(e)}")
            raise

    @staticmethod
    def _build_valid_result(query_job: Any) -> Dict[str, Any]:
        estimated_bytes = (
            query_job.total_bytes_processed
            if query_job.total_bytes_processed is not None
            else 0
        )
        success_message = (
            f"SQL syntax validated successfully (Estimated bytes: {estimated_bytes:,})"
        )
        logger.info(success_message)
        return {
            "valid": True,
            "message": success_message,
            "error_message": None,
            "details": {
                "estimated_bytes_processed": estimated_bytes,
                "job_id": query_job.job_id,
                "location": query_job.location,
            },
        }

    @staticmethod
    def _build_bad_request_result(exc: BadRequest) -> Dict[str, Any]:
        raw_error_str = str(exc)
        logger.warning(f"SQL validation failed (BadRequest): {raw_error_str}")
        sanitized_error = _sanitize_bq_error_message(raw_error_str)

        error_details: Dict[str, Any] = {"raw_error": sanitized_error}

        match_missing_field = re.search(
            r"Invalid field reference ['\"]([^'\"]+)['\"](?: \[at (\d+:\d+)\])?",
            sanitized_error,
            re.IGNORECASE,
        )
        if match_missing_field:
            error_details["missing_field"] = match_missing_field.group(1)
            if match_missing_field.group(2):
                error_details["error_location"] = match_missing_field.group(2)

        match_field_error = re.search(
            r"Invalid field name \"([^\"]+)\"(?: \[at (\d+:\d+)\])?",
            sanitized_error,
            re.IGNORECASE,
        )
        if match_field_error:
            error_details["invalid_field"] = match_field_error.group(1)
            if match_field_error.group(2):
                error_details["error_location"] = match_field_error.group(2)

        match_unrecognized_name = re.search(
            r"Unrecognized name: ([a-zA-Z0-9_.]+)(?: \[at (\d+:\d+)\])?",
            sanitized_error,
        )
        if match_unrecognized_name:
            error_details["unrecognized_name"] = match_unrecognized_name.group(1)
            if match_unrecognized_name.group(2):
                error_details["error_location"] = match_unrecognized_name.group(2)

        match_syntax_error = re.search(
            r"Syntax error: ([^\[]+)(?:\[at (\d+:\d+)\])?",
            sanitized_error,
        )
        if match_syntax_error:
            error_details["syntax_error_message"] = match_syntax_error.group(1).strip()
            if match_syntax_error.group(2):
                error_details["error_location"] = match_syntax_error.group(2)

        return {
            "valid": False,
            "message": "SQL validation failed.",
            "error": sanitized_error,
            "error_message": sanitized_error,
            "details": error_details,
        }

    @staticmethod
    def _build_access_or_not_found_result(exc: Exception) -> Dict[str, Any]:
        is_not_found = isinstance(exc, NotFound)
        reason = "not_found" if is_not_found else "access_denied"
        fallback = (
            "Referenced BigQuery dataset or table was not found."
            if is_not_found
            else "Access denied while validating SQL query."
        )
        raw_msg = (getattr(exc, "message", None) or str(exc)).strip()
        logger.warning(f"SQL validation failed ({reason}): {raw_msg}")
        sanitized_error = _sanitize_bq_error_message(raw_msg, fallback=fallback)
        return {
            "valid": False,
            "message": "SQL validation failed.",
            "error": sanitized_error,
            "error_message": sanitized_error,
            "details": {"reason": reason},
        }

    def validate_sql_dry_run(self, sql_script: str, timeout_seconds: int = 30) -> Dict[str, Any]:
        """
        Performs a dry run of a SQL query to validate it against BigQuery.
        (Logic from SQLFixService.validate_sql)

        Args:
            sql_script: The SQL script to validate.
            timeout_seconds: Timeout for the dry run in seconds.

        Returns:
            A dictionary containing validation results:
            {
                "valid": bool,
                "message": Optional[str], // Success message or general error
                "error_message": Optional[str], // Specific BigQuery error if invalid
                "details": Optional[Dict[str, Any]] // Additional details like estimated bytes or parsed error info
            }
        """
        if not sql_script or not sql_script.strip():
            logger.warning("SQL script is empty. Validation cannot be performed.")
            return {
                "valid": False,
                "message": "SQL script is empty.",
                "error_message": "SQL script is empty.",
                "details": None
            }

        logger.info(f"Validating SQL with dry run (timeout: {timeout_seconds}s)...")

        try:
            job_config = bigquery.QueryJobConfig(
                dry_run=True,
                use_query_cache=False  # Important for validation
            )

            try:
                query_job = self.bigquery_client.query(
                    sql_script,
                    job_config=job_config,
                    timeout=timeout_seconds,  # bigquery.Client.query timeout is in seconds
                )
                return self._build_valid_result(query_job)
            except (Forbidden, NotFound) as outer_exc:
                query_body = _extract_ctas_query_body(sql_script)
                if query_body is None:
                    return self._build_access_or_not_found_result(outer_exc)

                logger.info(
                    "CTAS dry run hit destination %s; retrying dry run on inner query body.",
                    type(outer_exc).__name__,
                )
                try:
                    inner_job = self.bigquery_client.query(
                        query_body,
                        job_config=job_config,
                        timeout=timeout_seconds,
                    )
                    return self._build_valid_result(inner_job)
                except BadRequest as inner_bad_req:
                    return self._build_bad_request_result(inner_bad_req)
                except (Forbidden, NotFound) as inner_perm_exc:
                    return self._build_access_or_not_found_result(inner_perm_exc)

        except BadRequest as e:
            return self._build_bad_request_result(e)

        except Exception as e:
            # Other unexpected errors (network issues, timeouts not caught by BadRequest, etc.)
            error_message_str = str(e)
            error_type = type(e).__name__
            logger.error(f"Unexpected error during SQL dry run ({error_type}): {error_message_str}", exc_info=True)
            return {
                "valid": False,
                "message": f"An unexpected error occurred during SQL validation: {error_type}.",
                "error_message": error_message_str,
                "details": {"error_type": error_type}
            }

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project:
        logger.error("GOOGLE_CLOUD_PROJECT environment variable not set. Skipping SQLValidator example.")
    else:
        validator = SQLValidator(project_id=project)

        print("\n--- Testing Valid SQL ---")
        valid_sql = "SELECT 1 AS one, 'test' AS two;"
        result_valid = validator.validate_sql_dry_run(valid_sql)
        print(f"Validation result for valid SQL: {json.dumps(result_valid, indent=2)}")

        print("\n--- Testing Invalid SQL (Syntax Error) ---")
        invalid_sql_syntax = "SELECT FROM table_that_does_not_exist;" # Syntax error before table existence check
        result_invalid_syntax = validator.validate_sql_dry_run(invalid_sql_syntax)
        print(f"Validation result for invalid syntax SQL: {json.dumps(result_invalid_syntax, indent=2)}")
        
        print("\n--- Testing Invalid SQL (Unrecognized Name) ---")
        # Assuming 'my_dataset.my_source_table' exists for this to primarily fail on 'non_existent_column'
        # If the table doesn't exist, the error might be different.
        # For a more reliable unrecognized name test, use a valid table.
        # Let's use a public dataset table for a more robust test.
        # This query is valid if the column exists, invalid if not.
        # This will test the "Unrecognized name" parsing.
        invalid_sql_field = "SELECT non_existent_column FROM `bigquery-public-data.samples.shakespeare` LIMIT 1;"
        result_invalid_field = validator.validate_sql_dry_run(invalid_sql_field)
        print(f"Validation result for non-existent column: {json.dumps(result_invalid_field, indent=2)}")

        print("\n--- Testing Empty SQL ---")
        empty_sql = "   "
        result_empty = validator.validate_sql_dry_run(empty_sql)
        print(f"Validation result for empty SQL: {json.dumps(result_empty, indent=2)}")
