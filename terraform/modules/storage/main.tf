resource "google_storage_bucket" "gcs_bucket" {
    project = var.gcp_project_id
    location = var.region
    name = var.gcs_bucket_name
}
resource "google_storage_bucket_object" "gcs_temp_folder" {
    name          = var.gcs_temp_folder
    content = "Placeholder for Dataflow to write temporary files"
    bucket        = "${google_storage_bucket.gcs_bucket.name}"
}
