variable "gcp_project_id" {
    description = "Centralized GCP project"
    type = string
}

variable "region" {
    type = string
    description = "Default location of GCP resources."
}

variable "gcs_bucket_name" {
    description = "GCS bucket name"
    type = string
}

variable "gcs_temp_folder" {
    description = "GCS temporary folder name"
    type = string
}
