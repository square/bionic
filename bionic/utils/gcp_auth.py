from bionic.deps.optdep import import_optional_dependency


def get_gcp_project_id():
    google_auth = import_optional_dependency(
        "google.auth", purpose="Get GCP project id from the environment"
    )
    _, project = google_auth.default()
    return project
