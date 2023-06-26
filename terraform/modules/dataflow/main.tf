resource "google_storage_bucket_object" "flex_template_metadata" {
    bucket       = var.gcs_bucket_name
    name         = "dataflow-templates/spec/metadata.json"
    content_type = "application/json"

    content = jsonencode({
        image = var.flex_template_image
        sdkInfo = {
            language = "PYTHON"
        }
        metadata = {
            name = "Apache Beam pipeline"
            parameters = [
                {
                    name = "config",
                    label = "config",
                    helpText = "Path for json file of pipline setting."
                },
                {
                    name = "profile",
                    label = "profile",
                    helpText = "Path for json file of profile information."
                },
                {
                    name = "gcs_temp_bucket",
                    label = "gcs_temp_bucket",
                    helpText = "Path for gcs bucket."
                }
            ]
        }
    })
}

resource "google_dataflow_flex_template_job" "flex_template" {
    provider                = google-beta
    name                    = var.dataflow_job_name
    region = var.region
    project = var.gcp_project_id
    container_spec_gcs_path = "gs://${var.gcs_bucket_name}/dataflow-templates/spec/metadata.json"
    parameters = {
        config = "messages"
        profile = "messages"
    }
}