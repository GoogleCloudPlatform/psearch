from pathlib import Path
import re
import hcl2

REPO_ROOT = Path(__file__).resolve().parents[4]
IAC_ROOT = REPO_ROOT / "src" / "iac"


def _extract_roles(for_each_expr: str) -> set[str]:
    return set(re.findall(r'"(roles/[^"]+)"', for_each_expr))


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

    assert (
        modules_by_name["spanner"]["service_account_email"]
        == "${module.iam.ingestion_service_account_email}"
    )
    assert (
        modules_by_name["ingestion_spanner"]["bq_dt_service_account_email"]
        == "${module.iam.ingestion_service_account_email}"
    )
    assert (
        modules_by_name["search_api"]["service_account_email"]
        == "${module.iam.search_api_service_account_email}"
    )
    assert (
        modules_by_name["ui"]["service_account_email"]
        == "${module.iam.ui_service_account_email}"
    )
    assert (
        modules_by_name["gen_ai"]["service_account_email"]
        == "${module.iam.gen_ai_service_account_email}"
    )


def test_no_cloud_run_module_allows_all_users_and_all_enforce_ingress():
    cloud_run_modules = {
        "gen_ai": "gen_ai_service",
        "ingestion_source": "ingestion_source_service",
        "search_api": "search_api_service",
        "ui": "ui_service",
    }

    for mod_name, service_res_name in cloud_run_modules.items():
        mod_dir = IAC_ROOT / "modules" / mod_name
        raw_main = (mod_dir / "main.tf").read_text(encoding="utf-8")
        assert "allUsers" not in raw_main, f"{mod_name}/main.tf still mentions allUsers"
        assert (
            "allAuthenticatedUsers" not in raw_main
        ), f"{mod_name}/main.tf still mentions allAuthenticatedUsers"

        parsed_main = _load_tf(mod_dir / "main.tf")
        policies = [
            r
            for r in parsed_main.get("resource", [])
            if "google_cloud_run_v2_service_iam_policy" in r
        ]
        assert policies == [], f"{mod_name} must not define google_cloud_run_v2_service_iam_policy"
        bindings = [
            r
            for r in parsed_main.get("resource", [])
            if "google_cloud_run_v2_service_iam_binding" in r
        ]
        assert bindings == [], f"{mod_name} must not define google_cloud_run_v2_service_iam_binding"
        iam_data = [
            d for d in parsed_main.get("data", []) if "google_iam_policy" in d
        ]
        assert iam_data == [], f"{mod_name} must not define google_iam_policy data source"

        members = [
            r["google_cloud_run_v2_service_iam_member"]
            for r in parsed_main.get("resource", [])
            if "google_cloud_run_v2_service_iam_member" in r
        ]
        assert len(members) == 1, f"{mod_name} must define invokers iam_member"
        invoker = members[0]["invokers"]
        assert invoker["project"] == "${var.project_id}"
        assert (
            invoker["location"]
            == f"${{google_cloud_run_v2_service.{service_res_name}.location}}"
        )
        assert (
            invoker["name"]
            == f"${{google_cloud_run_v2_service.{service_res_name}.name}}"
        )
        assert invoker["role"] == "roles/run.invoker"
        assert "toset(var.invoker_members)" in invoker["for_each"]
        assert "each.value" in invoker["member"]

        services = [
            r["google_cloud_run_v2_service"][service_res_name]
            for r in parsed_main.get("resource", [])
            if "google_cloud_run_v2_service" in r
        ]
        assert len(services) == 1
        assert services[0].get("ingress") == "${var.ingress}"

        parsed_vars = _load_tf(mod_dir / "variables.tf")
        vars_by_name = {
            k: v for item in parsed_vars.get("variable", []) for k, v in item.items()
        }
        assert "invoker_members" in vars_by_name
        assert vars_by_name["invoker_members"]["default"] == []
        assert "allUsers" not in vars_by_name["invoker_members"].get("description", "")
        assert (
            "allAuthenticatedUsers"
            not in vars_by_name["invoker_members"].get("description", "")
        )
        invoker_validations = vars_by_name["invoker_members"].get("validation", [])
        assert len(invoker_validations) >= 1
        invoker_cond = invoker_validations[0]["condition"]
        assert "allUsers" in invoker_cond and "allAuthenticatedUsers" in invoker_cond

        assert "ingress" in vars_by_name
        assert (
            vars_by_name["ingress"]["default"]
            == "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"
        )
        ingress_validations = vars_by_name["ingress"].get("validation", [])
        assert len(ingress_validations) >= 1
        ingress_cond = ingress_validations[0]["condition"]
        for allowed_ingress in (
            "INGRESS_TRAFFIC_INTERNAL_ONLY",
            "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER",
            "INGRESS_TRAFFIC_ALL",
        ):
            assert allowed_ingress in ingress_cond

    # Verify gen_ai runtime auth/CORS env blocks & module variables
    gen_ai_main = _load_tf(IAC_ROOT / "modules" / "gen_ai" / "main.tf")
    gen_ai_svc = [
        r["google_cloud_run_v2_service"]["gen_ai_service"]
        for r in gen_ai_main.get("resource", [])
        if "google_cloud_run_v2_service" in r
    ][0]
    gen_ai_envs = {
        e["name"]: e["value"]
        for e in gen_ai_svc["template"][0]["containers"][0]["env"]
    }
    assert gen_ai_envs["GEN_AI_AUTH_AUDIENCE"] == "${var.gen_ai_auth_audience}"
    assert (
        gen_ai_envs["GEN_AI_ALLOWED_PRINCIPALS"] == "${var.gen_ai_allowed_principals}"
    )
    assert gen_ai_envs["GEN_AI_ALLOWED_DOMAINS"] == "${var.gen_ai_allowed_domains}"
    assert gen_ai_envs["ALLOWED_ORIGINS"] == "${var.allowed_origins}"
    assert (
        gen_ai_envs["GEN_AI_RATE_LIMIT_PER_MINUTE"]
        == "${tostring(var.rate_limit_per_minute)}"
    )
    assert (
        gen_ai_envs["GEN_AI_SQL_TASK_RATE_LIMIT_PER_MINUTE"]
        == "${tostring(var.sql_task_rate_limit_per_minute)}"
    )
    assert (
        gen_ai_envs["GEN_AI_MAX_CONCURRENT_SQL_TASKS"]
        == "${tostring(var.max_concurrent_sql_tasks)}"
    )

    gen_ai_vars = {
        k: v
        for item in _load_tf(IAC_ROOT / "modules" / "gen_ai" / "variables.tf").get(
            "variable", []
        )
        for k, v in item.items()
    }
    for var_name in (
        "gen_ai_auth_audience",
        "gen_ai_allowed_principals",
        "gen_ai_allowed_domains",
        "allowed_origins",
    ):
        assert gen_ai_vars[var_name]["default"] == ""
    expected_gen_ai_limits = {
        "rate_limit_per_minute": 60,
        "sql_task_rate_limit_per_minute": 10,
        "max_concurrent_sql_tasks": 3,
    }
    for var_name, expected_default in expected_gen_ai_limits.items():
        assert gen_ai_vars[var_name]["default"] == expected_default
        validations = gen_ai_vars[var_name].get("validation", [])
        assert len(validations) >= 1
        assert "> 0" in validations[0]["condition"]

    # Verify search_api runtime auth/CORS env blocks & module variables
    search_api_main = _load_tf(IAC_ROOT / "modules" / "search_api" / "main.tf")
    search_api_svc = [
        r["google_cloud_run_v2_service"]["search_api_service"]
        for r in search_api_main.get("resource", [])
        if "google_cloud_run_v2_service" in r
    ][0]
    search_api_envs = {
        e["name"]: e["value"]
        for e in search_api_svc["template"][0]["containers"][0]["env"]
    }
    assert search_api_envs["REQUIRE_API_AUTH"] == "${var.require_api_auth}"
    assert search_api_envs["SERVING_AUTH_AUDIENCE"] == "${var.serving_auth_audience}"
    assert (
        search_api_envs["SERVING_ALLOWED_PRINCIPALS"]
        == "${var.serving_allowed_principals}"
    )
    assert (
        search_api_envs["SERVING_ALLOWED_DOMAINS"] == "${var.serving_allowed_domains}"
    )
    assert search_api_envs["ALLOWED_ORIGINS"] == "${var.allowed_origins}"

    search_api_vars = {
        k: v
        for item in _load_tf(IAC_ROOT / "modules" / "search_api" / "variables.tf").get(
            "variable", []
        )
        for k, v in item.items()
    }
    assert search_api_vars["require_api_auth"]["default"] == "true"
    for var_name in (
        "serving_auth_audience",
        "serving_allowed_principals",
        "serving_allowed_domains",
        "allowed_origins",
    ):
        assert search_api_vars[var_name]["default"] == ""

    # Verify root variables & wiring in src/iac/variables.tf and src/iac/main.tf
    root_vars = {
        k: v
        for item in _load_tf(IAC_ROOT / "variables.tf").get("variable", [])
        for k, v in item.items()
    }
    for invoker_var in (
        "gen_ai_invoker_members",
        "search_api_invoker_members",
        "ui_invoker_members",
        "ingestion_source_invoker_members",
    ):
        assert invoker_var in root_vars
        assert root_vars[invoker_var]["default"] == []
        validations = root_vars[invoker_var].get("validation", [])
        assert len(validations) >= 1
        assert (
            "allUsers" in validations[0]["condition"]
            and "allAuthenticatedUsers" in validations[0]["condition"]
        )

    for ingress_var in (
        "gen_ai_ingress",
        "search_api_ingress",
        "ui_ingress",
        "ingestion_source_ingress",
    ):
        assert ingress_var in root_vars
        assert (
            root_vars[ingress_var]["default"]
            == "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"
        )
        validations = root_vars[ingress_var].get("validation", [])
        assert len(validations) >= 1
        for allowed_ingress in (
            "INGRESS_TRAFFIC_INTERNAL_ONLY",
            "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER",
            "INGRESS_TRAFFIC_ALL",
        ):
            assert allowed_ingress in validations[0]["condition"]

    assert root_vars["search_api_require_auth"]["default"] == "true"
    for empty_var in (
        "gen_ai_auth_audience",
        "gen_ai_allowed_principals",
        "gen_ai_allowed_domains",
        "gen_ai_allowed_origins",
        "serving_auth_audience",
        "serving_allowed_principals",
        "serving_allowed_domains",
        "search_api_allowed_origins",
    ):
        assert root_vars[empty_var]["default"] == ""

    root_modules = {
        k: v
        for item in _load_tf(IAC_ROOT / "main.tf").get("module", [])
        for k, v in item.items()
    }
    assert root_modules["gen_ai"]["invoker_members"] == "${var.gen_ai_invoker_members}"
    assert root_modules["gen_ai"]["ingress"] == "${var.gen_ai_ingress}"
    assert root_modules["gen_ai"]["gen_ai_auth_audience"] == "${var.gen_ai_auth_audience}"
    assert (
        root_modules["gen_ai"]["gen_ai_allowed_principals"]
        == "${var.gen_ai_allowed_principals}"
    )
    assert (
        root_modules["gen_ai"]["gen_ai_allowed_domains"]
        == "${var.gen_ai_allowed_domains}"
    )
    assert root_modules["gen_ai"]["allowed_origins"] == "${var.gen_ai_allowed_origins}"
    assert root_vars["gen_ai_rate_limit_per_minute"]["default"] == 60
    assert root_vars["gen_ai_sql_task_rate_limit_per_minute"]["default"] == 10
    assert root_vars["gen_ai_max_concurrent_sql_tasks"]["default"] == 3
    for root_limit_var in (
        "gen_ai_rate_limit_per_minute",
        "gen_ai_sql_task_rate_limit_per_minute",
        "gen_ai_max_concurrent_sql_tasks",
    ):
        validations = root_vars[root_limit_var].get("validation", [])
        assert len(validations) >= 1
        assert "> 0" in validations[0]["condition"]
    assert (
        root_modules["gen_ai"]["rate_limit_per_minute"]
        == "${var.gen_ai_rate_limit_per_minute}"
    )
    assert (
        root_modules["gen_ai"]["sql_task_rate_limit_per_minute"]
        == "${var.gen_ai_sql_task_rate_limit_per_minute}"
    )
    assert (
        root_modules["gen_ai"]["max_concurrent_sql_tasks"]
        == "${var.gen_ai_max_concurrent_sql_tasks}"
    )

    assert (
        root_modules["search_api"]["invoker_members"]
        == "${var.search_api_invoker_members}"
    )
    assert root_modules["search_api"]["ingress"] == "${var.search_api_ingress}"
    assert (
        root_modules["search_api"]["require_api_auth"]
        == "${var.search_api_require_auth}"
    )
    assert (
        root_modules["search_api"]["serving_auth_audience"]
        == "${var.serving_auth_audience}"
    )
    assert (
        root_modules["search_api"]["serving_allowed_principals"]
        == "${var.serving_allowed_principals}"
    )
    assert (
        root_modules["search_api"]["serving_allowed_domains"]
        == "${var.serving_allowed_domains}"
    )
    assert (
        root_modules["search_api"]["allowed_origins"]
        == "${var.search_api_allowed_origins}"
    )

    assert root_modules["ui"]["invoker_members"] == "${var.ui_invoker_members}"
    assert root_modules["ui"]["ingress"] == "${var.ui_ingress}"
    assert (
        root_modules["ingestion_source"]["ingress"] == "${var.ingestion_source_ingress}"
    )


def test_dedicated_least_privilege_service_accounts_for_every_runtime_service():
    iam_tf = _load_tf(IAC_ROOT / "modules" / "iam" / "main.tf")
    sas = {
        k: v
        for r in iam_tf.get("resource", [])
        if "google_service_account" in r
        for k, v in r["google_service_account"].items()
    }
    expected_sas = {
        "gen_ai_service_account": "gen-ai-sa",
        "search_api_service_account": "search-api-sa",
        "ui_service_account": "psearch-ui-sa",
        "ingestion_source_service_account": "ingestion-source-sa",
    }
    for res_name, account_id in expected_sas.items():
        assert res_name in sas, f"Missing google_service_account.{res_name}"
        assert sas[res_name]["account_id"] == account_id

    iam_outputs = {
        k: v
        for item in _load_tf(IAC_ROOT / "modules" / "iam" / "outputs.tf").get(
            "output", []
        )
        for k, v in item.items()
    }
    for output_name in (
        "gen_ai_service_account_email",
        "search_api_service_account_email",
        "ui_service_account_email",
        "ingestion_source_service_account_email",
    ):
        assert output_name in iam_outputs

    project_iam = {
        k: v
        for r in iam_tf.get("resource", [])
        if "google_project_iam_member" in r
        for k, v in r["google_project_iam_member"].items()
    }

    # gen-ai-sa project roles
    assert "grant_gen_ai_sa_roles" in project_iam
    assert (
        project_iam["grant_gen_ai_sa_roles"]["member"]
        == "serviceAccount:${google_service_account.gen_ai_service_account.email}"
    )
    assert _extract_roles(project_iam["grant_gen_ai_sa_roles"]["for_each"]) == {
        "roles/aiplatform.user",
        "roles/bigquery.jobUser",
        "roles/logging.logWriter",
        "roles/serviceusage.serviceUsageConsumer",
    }

    # search-api-sa project roles
    assert "grant_search_api_sa_roles" in project_iam
    assert (
        project_iam["grant_search_api_sa_roles"]["member"]
        == "serviceAccount:${google_service_account.search_api_service_account.email}"
    )
    assert _extract_roles(project_iam["grant_search_api_sa_roles"]["for_each"]) == {
        "roles/aiplatform.user",
        "roles/logging.logWriter",
        "roles/serviceusage.serviceUsageConsumer",
    }

    # psearch-ui-sa project roles
    assert "grant_ui_sa_roles" in project_iam
    assert (
        project_iam["grant_ui_sa_roles"]["member"]
        == "serviceAccount:${google_service_account.ui_service_account.email}"
    )
    assert _extract_roles(project_iam["grant_ui_sa_roles"]["for_each"]) == {
        "roles/logging.logWriter",
    }

    # ingestion_service_account trimmed least-privilege roles
    assert "grant_sa_function_roles" in project_iam
    ingestion_roles = _extract_roles(project_iam["grant_sa_function_roles"]["for_each"])
    assert ingestion_roles == {
        "roles/bigquery.user",
        "roles/spanner.databaseUser",
        "roles/aiplatform.user",
        "roles/logging.logWriter",
        "roles/serviceusage.serviceUsageConsumer",
    }
    forbidden_roles = {
        "roles/storage.objectAdmin",
        "roles/spanner.databaseAdmin",
        "roles/datastore.owner",
        "roles/cloudfunctions.developer",
        "roles/run.developer",
        "roles/cloudbuild.builds.builder",
    }
    assert ingestion_roles.isdisjoint(forbidden_roles)

    # Root module wiring + scoped dataset and Spanner database IAM
    root_parsed = _load_tf(IAC_ROOT / "main.tf")
    modules_by_name = {
        k: v for item in root_parsed.get("module", []) for k, v in item.items()
    }
    assert (
        modules_by_name["gen_ai"]["service_account_email"]
        == "${module.iam.gen_ai_service_account_email}"
    )
    assert (
        modules_by_name["search_api"]["service_account_email"]
        == "${module.iam.search_api_service_account_email}"
    )
    assert (
        modules_by_name["ui"]["service_account_email"]
        == "${module.iam.ui_service_account_email}"
    )
    assert (
        modules_by_name["ingestion_source"]["service_account_email"]
        == "${module.iam.ingestion_source_service_account_email}"
    )

    # Dataset-scoped roles/bigquery.dataViewer on psearch_raw and psearch for gen-ai-sa
    bq_dataset_iam = [
        v
        for r in root_parsed.get("resource", [])
        if "google_bigquery_dataset_iam_member" in r
        for v in r["google_bigquery_dataset_iam_member"].values()
    ]
    gen_ai_bq_bindings = [
        b
        for b in bq_dataset_iam
        if b.get("member")
        == "serviceAccount:${module.iam.gen_ai_service_account_email}"
    ]
    assert len(gen_ai_bq_bindings) == 2
    assert {b["dataset_id"] for b in gen_ai_bq_bindings} == {"psearch_raw", "psearch"}
    for binding in gen_ai_bq_bindings:
        assert binding["project"] == "${var.project_id}"
        assert binding["role"] == "roles/bigquery.dataViewer"
        assert any(
            "module.ingestion_spanner" in str(dep)
            for dep in binding.get("depends_on", [])
        )

    # Database-scoped google_spanner_database_iam_member for search-api-sa
    spanner_db_iam = [
        v
        for r in root_parsed.get("resource", [])
        if "google_spanner_database_iam_member" in r
        for v in r["google_spanner_database_iam_member"].values()
    ]
    search_api_spanner_bindings = [
        b
        for b in spanner_db_iam
        if b.get("member")
        == "serviceAccount:${module.iam.search_api_service_account_email}"
    ]
    assert len(search_api_spanner_bindings) == 1
    spanner_binding = search_api_spanner_bindings[0]
    assert spanner_binding["project"] == "${var.project_id}"
    assert spanner_binding["instance"] == "${module.spanner.instance_id}"
    assert spanner_binding["database"] == "${module.spanner.database_id}"
    assert spanner_binding["role"] == "roles/spanner.databaseUser"
    assert any(
        "module.spanner" in str(dep) for dep in spanner_binding.get("depends_on", [])
    )

    # Verify modules/spanner/main.tf scopes spanner_user to the database instead of project-wide
    spanner_mod_tf = _load_tf(IAC_ROOT / "modules" / "spanner" / "main.tf")
    spanner_project_iam = [
        r
        for r in spanner_mod_tf.get("resource", [])
        if "google_project_iam_member" in r
    ]
    assert spanner_project_iam == []
    spanner_mod_db_iam = [
        v
        for r in spanner_mod_tf.get("resource", [])
        if "google_spanner_database_iam_member" in r
        for v in r["google_spanner_database_iam_member"].values()
    ]
    assert len(spanner_mod_db_iam) == 1
    assert spanner_mod_db_iam[0]["role"] == "roles/spanner.databaseUser"
    assert (
        spanner_mod_db_iam[0]["instance"]
        == "${google_spanner_instance.psearch_spanner.name}"
    )
    assert (
        spanner_mod_db_iam[0]["database"]
        == "${google_spanner_database.products_db.name}"
    )


def test_all_iac_tf_files_parse_cleanly():
    tf_files = sorted(IAC_ROOT.rglob("*.tf"))
    assert len(tf_files) >= 18
    for tf_file in tf_files:
        parsed = _load_tf(tf_file)
        assert isinstance(parsed, dict), f"Failed to parse {tf_file}"
