variable "gcp_project_id" {
  type = string
  description = "The ID of the GCP project."
}
variable "required_apis" {
  type    = list(string)
  default = []
}