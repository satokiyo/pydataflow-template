variable "gcp_project_id" {
  type = string
  description = "The ID of the GCP project."
}

variable "gcp_project_number" {
  type = string
  description = "The numeric identifier of the project."
}

variable "iam_members" {
  type = list(object({
    role    = string
    principals = list(string)
  }))
  description = "A list of dictionaries representing the role and principals."
}
