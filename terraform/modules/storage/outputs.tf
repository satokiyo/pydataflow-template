output "gcs_bucket_name" {
    value = google_storage_bucket.gcs_bucket.name
}

output "gcs_bucket_url" {
    value = google_storage_bucket.gcs_bucket.url
}

output "gcs_temp_folder_name" {
    value = google_storage_bucket_object.gcs_temp_folder.name
}

