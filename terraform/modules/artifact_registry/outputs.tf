data "google_artifact_registry_repository" "repository" {
    location = google_artifact_registry_repository.repository.location
    repository_id     = google_artifact_registry_repository.repository.repository_id
}

output "repository_id" {
    value = data.google_artifact_registry_repository.repository.id
}

output "repository_name" {
    value = data.google_artifact_registry_repository.repository.name
}