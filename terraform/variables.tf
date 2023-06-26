variable "gcp_project_id" {
    type = string
    description = "The ID of the GCP project."
}

variable "region" {
    type = string
    description = "Default location of GCP resources."
}

variable "gcp_project_name" {
    type = string
    description = "The name of the GCP project."
}

variable "gcp_billing_account_id" {
    type = string
    description = "ID of billing account."
}

variable "gcs_bucket_name" {
    type = string
    description = "Name of GCS bucket."
}

variable "gcs_temp_folder" {
    type = string
    description = "Name of GCS temp folder."
}

variable "repository_name" {
    type = string
    description = "Name of artifact registry repository."
}

variable "dataflow_worker_service_account" {
    type        = string
    description = "Name of Dataflow worker service account to be created and used to execute job operations. In the default case of creating a new service account (`use_externally_managed_dataflow_sa=false`), this parameter must be 6-30 characters long, and match the regular expression [a-z]([-a-z0-9]*[a-z0-9]). If the parameter is empty, worker service account defaults to project's Compute Engine default service account. If using external service account (`use_externally_managed_dataflow_sa=true`), this parameter must be the full email address of the external service account."
    default     = ""

    validation {
        condition = (var.dataflow_worker_service_account == "" ||
            can(regex("[a-z]([-a-z0-9]*[a-z0-9])", var.dataflow_worker_service_account)) ||
            can(regex("[a-z]([-a-z0-9]*[a-z0-9])@[a-z]([-a-z0-9]*[a-z0-9])(\\.iam)?.gserviceaccount.com$", var.dataflow_worker_service_account))
        )
        error_message = "Dataflow worker service account id must match the regular expression '[a-z]([-a-z0-9]*[a-z0-9])' in case of service account name, or '[a-z]([-a-z0-9]*[a-z0-9])@[a-z]([-a-z0-9]*[a-z0-9])(\\.iam)?.gserviceaccount.com$' in case of service account email address."
    }
}

variable "use_externally_managed_dataflow_sa" {
    type        = bool
    default     = false
    description = "Determines if the worker service account provided by `dataflow_worker_service_account` variable should be created by this module (default) or is managed outside of the module. In the latter case, user is expected to apply and manage the service account IAM permissions over external resources (e.g. Cloud KMS key or Secret version) before running this module."
}
variable "workflows_service_account" {
    type        = string
    description = "Name of workflows worker service account to be created and used to execute job operations. In the default case of creating a new service account (`use_externally_managed_workflows_sa=false`), this parameter must be 6-30 characters long, and match the regular expression [a-z]([-a-z0-9]*[a-z0-9]). If the parameter is empty, worker service account defaults to project's Compute Engine default service account. If using external service account (`use_externally_managed_workflows_sa=true`), this parameter must be the full email address of the external service account."
    default     = ""

    validation {
        condition = (var.workflows_service_account == "" ||
            can(regex("[a-z]([-a-z0-9]*[a-z0-9])", var.workflows_service_account)) ||
            can(regex("[a-z]([-a-z0-9]*[a-z0-9])@[a-z]([-a-z0-9]*[a-z0-9])(\\.iam)?.gserviceaccount.com$", var.workflows_service_account))
        )
        error_message = "Workflows service account id must match the regular expression '[a-z]([-a-z0-9]*[a-z0-9])' in case of service account name, or '[a-z]([-a-z0-9]*[a-z0-9])@[a-z]([-a-z0-9]*[a-z0-9])(\\.iam)?.gserviceaccount.com$' in case of service account email address."
    }
}

variable "use_externally_managed_workflows_sa" {
    type        = bool
    default     = false
    description = "Determines if the worker service account provided by `workflows_service_account` variable should be created by this module (default) or is managed outside of the module. In the latter case, user is expected to apply and manage the service account IAM permissions over external resources (e.g. Cloud KMS key or Secret version) before running this module."
}
