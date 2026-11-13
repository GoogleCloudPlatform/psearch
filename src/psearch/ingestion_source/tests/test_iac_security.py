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
    env_names = {e["name"] for e in envs}
    assert {
        "INGESTION_AUTH_AUDIENCE",
        "INGESTION_ALLOWED_PRINCIPALS",
        "ALLOWED_ORIGINS",
        "MAX_UPLOAD_SIZE_BYTES",
    }.issubset(env_names)


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

    bucket_iam = [
        r["google_storage_bucket_iam_member"]
        for r in iam_tf.get("resource", [])
        if "google_storage_bucket_iam_member" in r
    ]
    assert any("ingestion_source_raw_bucket_admin" in m for m in bucket_iam)

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
    assert "roles/spanner.databaseAdmin" not in sa_roles_expr

    iam_outputs = _load_tf(IAC_ROOT / "modules" / "iam" / "outputs.tf")
    outputs_by_name = {k: v for item in iam_outputs.get("output", []) for k, v in item.items()}
    assert "ingestion_source_service_account_email" in outputs_by_name

    root_tf = (IAC_ROOT / "main.tf").read_text(encoding="utf-8")
    assert "module.iam.ingestion_source_service_account_email" in root_tf
    root_parsed = _load_tf(IAC_ROOT / "main.tf")
    modules_by_name = {k: v for item in root_parsed.get("module", []) for k, v in item.items()}
    assert (
        modules_by_name["ingestion_source"]["service_account_email"]
        == "${module.iam.ingestion_source_service_account_email}"
    )
    assert (
        modules_by_name["ingestion_source"]["invoker_members"]
        == "${var.ingestion_source_invoker_members}"
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
