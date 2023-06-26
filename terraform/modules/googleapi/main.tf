resource "google_project_service" "project" {
  for_each = toset(var.required_apis)
  project = var.gcp_project_id
  service = each.key
}