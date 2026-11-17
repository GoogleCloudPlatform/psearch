from pathlib import Path
import hcl2

REPO_ROOT = Path(__file__).resolve().parents[4]
IAC_ROOT = REPO_ROOT / "src" / "iac"


def _load_tf(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        if hasattr(hcl2, "SerializationOptions"):
            return hcl2.load(
                fh,
                serialization_options=hcl2.SerializationOptions(
                    strip_string_quotes=True,
                    explicit_blocks=False,
                ),
            )
        return hcl2.load(fh)


def test_ingestion_source_does_not_grant_all_users_invoker():
    raw = (IAC_ROOT / "modules" / "ingestion_source" / "main.tf").read_text(encoding="utf-8")
    parsed = _load_tf(IAC_ROOT / "modules" / "ingestion_source" / "main.tf")
    assert '"allUsers"' not in raw
    assert '"allAuthenticatedUsers"' not in raw
    policies = [
        r for r in parsed.get("resource", [])
        if "google_cloud_run_v2_service_iam_policy" in r
    ]
    assert policies == []
    bindings = [
        r for r in parsed.get("resource", [])
        if "google_cloud_run_v2_service_iam_binding" in r
    ]
    assert bindings == []
    iam_data = [
        d for d in parsed.get("data", [])
        if "google_iam_policy" in d
    ]
    assert iam_data == []
    members = [
        r["google_cloud_run_v2_service_iam_member"]
        for r in parsed.get("resource", [])
        if "google_cloud_run_v2_service_iam_member" in r
    ]
    assert len(members) == 1
    invoker = members[0]["invokers"]
    assert invoker["role"] == "roles/run.invoker"
    assert "toset(var.invoker_members)" in invoker["for_each"]
    assert "each.value" in invoker["member"]

    services = [
        r["google_cloud_run_v2_service"]["ingestion_source_service"]
        for r in parsed.get("resource", [])
        if "google_cloud_run_v2_service" in r
    ]
    assert len(services) == 1
    envs = services[0]["template"][0]["containers"][0]["env"]
    env_by_name = {e["name"]: e["value"] for e in envs}
    assert {
        "INGESTION_AUTH_AUDIENCE",
        "INGESTION_ALLOWED_PRINCIPALS",
        "INGESTION_ALLOWED_DOMAINS",
        "ALLOWED_ORIGINS",
        "MAX_UPLOAD_SIZE_BYTES",
    }.issubset(env_by_name.keys())
    assert env_by_name["INGESTION_ALLOWED_DOMAINS"] == "${var.ingestion_allowed_domains}"


def test_ingestion_source_invoker_members_defaults_empty_and_rejects_public():
    parsed = _load_tf(IAC_ROOT / "modules" / "ingestion_source" / "variables.tf")
    vars_by_name = {k: v for item in parsed.get("variable", []) for k, v in item.items()}
    assert "invoker_members" in vars_by_name
    assert vars_by_name["invoker_members"]["default"] == []
    validations = vars_by_name["invoker_members"].get("validation", [])
    assert len(validations) >= 1
    cond = validations[0]["condition"]
    assert "allUsers" in cond and "allAuthenticatedUsers" in cond

    assert vars_by_name["ingestion_auth_audience"]["default"] == ""
    assert vars_by_name["ingestion_allowed_principals"]["default"] == ""
    assert vars_by_name["ingestion_allowed_domains"]["default"] == ""
    assert vars_by_name["allowed_origins"]["default"] == ""
    assert vars_by_name["max_upload_size_bytes"]["default"] == 52428800

    root_vars_parsed = _load_tf(IAC_ROOT / "variables.tf")
    root_vars = {k: v for item in root_vars_parsed.get("variable", []) for k, v in item.items()}
    assert "ingestion_source_invoker_members" in root_vars
    assert root_vars["ingestion_source_invoker_members"]["default"] == []
    root_validations = root_vars["ingestion_source_invoker_members"].get("validation", [])
    assert len(root_validations) >= 1
    root_cond = root_validations[0]["condition"]
    assert "allUsers" in root_cond and "allAuthenticatedUsers" in root_cond

    assert root_vars["ingestion_auth_audience"]["default"] == ""
    assert root_vars["ingestion_allowed_principals"]["default"] == ""
    assert root_vars["ingestion_allowed_domains"]["default"] == ""
    assert root_vars["ingestion_allowed_origins"]["default"] == ""
    assert root_vars["ingestion_max_upload_size_bytes"]["default"] == 52428800


def test_dedicated_ingestion_source_sa_and_scoped_bucket_iam():
    iam_tf = _load_tf(IAC_ROOT / "modules" / "iam" / "main.tf")
    sas = {
        k: v
        for r in iam_tf.get("resource", [])
        if "google_service_account" in r
        for k, v in r["google_service_account"].items()
    }
    assert "ingestion_service_account" in sas
    assert "ingestion_source_service_account" in sas
    assert sas["ingestion_source_service_account"]["account_id"] == "ingestion-source-sa"

    buckets = {
        k: v
        for r in iam_tf.get("resource", [])
        if "google_storage_bucket" in r
        for k, v in r["google_storage_bucket"].items()
    }
    assert "psearch_raw" in buckets
    assert buckets["psearch_raw"]["name"] == "${var.project_id}_psearch_raw"
    assert buckets["psearch_raw"]["uniform_bucket_level_access"] is True
    assert buckets["psearch_raw"]["public_access_prevention"] == "enforced"

    bucket_iam_by_name = {
        k: v
        for r in iam_tf.get("resource", [])
        if "google_storage_bucket_iam_member" in r
        for k, v in r["google_storage_bucket_iam_member"].items()
    }
    expected_bucket_ref = "${google_storage_bucket.psearch_raw.name}"
    expected_sa_member = (
        "serviceAccount:${google_service_account.ingestion_source_service_account.email}"
    )
    assert "ingestion_source_raw_bucket_admin" in bucket_iam_by_name
    admin_binding = bucket_iam_by_name["ingestion_source_raw_bucket_admin"]
    assert admin_binding["bucket"] == expected_bucket_ref
    assert admin_binding["role"] == "roles/storage.objectAdmin"
    assert admin_binding["member"] == expected_sa_member

    assert "ingestion_source_raw_bucket_reader" in bucket_iam_by_name
    reader_binding = bucket_iam_by_name["ingestion_source_raw_bucket_reader"]
    assert reader_binding["bucket"] == expected_bucket_ref
    assert reader_binding["role"] == "roles/storage.legacyBucketReader"
    assert reader_binding["member"] == expected_sa_member

    project_iam = {
        k: v
        for r in iam_tf.get("resource", [])
        if "google_project_iam_member" in r
        for k, v in r["google_project_iam_member"].items()
    }
    assert "grant_ingestion_source_sa_roles" in project_iam
    sa_roles_expr = project_iam["grant_ingestion_source_sa_roles"]["for_each"]
    assert "roles/bigquery.user" in sa_roles_expr
    assert "roles/logging.logWriter" in sa_roles_expr
    assert "roles/serviceusage.serviceUsageConsumer" in sa_roles_expr
    assert "roles/storage.objectAdmin" not in sa_roles_expr
    assert "roles/bigquery.dataEditor" not in sa_roles_expr
    assert "roles/spanner.databaseAdmin" not in sa_roles_expr

    iam_outputs = _load_tf(IAC_ROOT / "modules" / "iam" / "outputs.tf")
    outputs_by_name = {k: v for item in iam_outputs.get("output", []) for k, v in item.items()}
    assert "ingestion_source_service_account_email" in outputs_by_name

    root_tf = (IAC_ROOT / "main.tf").read_text(encoding="utf-8")
    assert "module.iam.ingestion_source_service_account_email" in root_tf
    root_parsed = _load_tf(IAC_ROOT / "main.tf")
    modules_by_name = {k: v for item in root_parsed.get("module", []) for k, v in item.items()}
    ingestion_mod = modules_by_name["ingestion_source"]
    assert (
        ingestion_mod["service_account_email"]
        == "${module.iam.ingestion_source_service_account_email}"
    )
    assert (
        ingestion_mod["invoker_members"]
        == "${var.ingestion_source_invoker_members}"
    )
    assert (
        ingestion_mod["ingestion_auth_audience"]
        == "${var.ingestion_auth_audience}"
    )
    assert (
        ingestion_mod["ingestion_allowed_principals"]
        == "${var.ingestion_allowed_principals}"
    )
    assert (
        ingestion_mod["ingestion_allowed_domains"]
        == "${var.ingestion_allowed_domains}"
    )
    assert (
        ingestion_mod["allowed_origins"]
        == "${var.ingestion_allowed_origins}"
    )
    assert (
        ingestion_mod["max_upload_size_bytes"]
        == "${var.ingestion_max_upload_size_bytes}"
    )

    bq_dataset_iam_by_name = {
        k: v
        for r in root_parsed.get("resource", [])
        if "google_bigquery_dataset_iam_member" in r
        for k, v in r["google_bigquery_dataset_iam_member"].items()
    }
    assert "ingestion_source_psearch_raw_editor" in bq_dataset_iam_by_name
    bq_editor = bq_dataset_iam_by_name["ingestion_source_psearch_raw_editor"]
    assert bq_editor["project"] == "${var.project_id}"
    assert bq_editor["dataset_id"] == "psearch_raw"
    assert bq_editor["role"] == "roles/bigquery.dataEditor"
    assert (
        bq_editor["member"]
        == "serviceAccount:${module.iam.ingestion_source_service_account_email}"
    )
    assert any(
        "module.ingestion_spanner" in str(dep)
        for dep in bq_editor.get("depends_on", [])
    )

    for shared_mod in ("spanner", "search_api", "ui", "gen_ai"):
        assert (
            modules_by_name[shared_mod]["service_account_email"]
            == "${module.iam.ingestion_service_account_email}"
        )
    assert (
        modules_by_name["ingestion_spanner"]["bq_dt_service_account_email"]
        == "${module.iam.ingestion_service_account_email}"
    )
