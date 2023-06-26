variable "gcp_project_id" {
    type = string
    description = "The ID of the GCP project."
}

variable "repository_name" {
    type = string
    description = "The name of repository."
}

variable "region" {
    type = string
    description = "Default location of GCP resources."
}

variable "format" {
    default = "DOCKER"
}