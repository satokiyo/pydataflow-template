variable "gcp_project_id" {
    type = string
    description = "The ID of the GCP project."
}

variable "region" {
    type = string
    description = "Default location of GCP resources."
}

variable "dataflow_job_name" {
    type = string
    description = "The name of the Dataflow job."
}
variable "gcs_bucket_name" {
    type = string
    description = "The name of the GCS bucket."
}
variable "flex_template_image" {
    type = string
    description = "flex template image."
}
