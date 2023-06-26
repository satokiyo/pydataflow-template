provider "google" {
  project = var.gcp_project_id
  region  = var.region
}
resource "google_project" "project" {
  name = var.gcp_project_name
  project_id = var.gcp_project_id
  billing_account = var.gcp_billing_account_id
  auto_create_network = false
}

resource "google_service_account" "dataflow_worker_service_account" {
  count        = (var.dataflow_worker_service_account != "" && var.use_externally_managed_dataflow_sa == false) ? 1 : 0
  project      = var.gcp_project_id
  account_id   = var.dataflow_worker_service_account
  display_name = "Dataflow worker service account to execute pipeline operations"
}

resource "google_service_account" "workflows_service_account" {
  count        = (var.workflows_service_account != "" && var.use_externally_managed_workflows_sa == false) ? 1 : 0
  project      = var.gcp_project_id
  account_id   = var.workflows_service_account
  display_name = "Workflows service account to invoke dataflow jobs."
}

data "google_project" "project" {
  project_id = var.gcp_project_id
}
locals {
  # GCP
  gcp_project_id = data.google_project.project.project_id
  gcp_project_number = data.google_project.project.number
  region  = var.region

  # GCS
  gcs_temp_folder = var.gcs_temp_folder

  # Service Account
  dataflow_worker_service_account = ((var.dataflow_worker_service_account != "") ?
    ((var.use_externally_managed_dataflow_sa == false) ?
      google_service_account.dataflow_worker_service_account[0].email :
    var.dataflow_worker_service_account) :
  "${local.gcp_project_number}-compute@developer.gserviceaccount.com")

  workflows_service_account = ((var.workflows_service_account != "") ?
    ((var.use_externally_managed_workflows_sa == false) ?
      google_service_account.workflows_service_account[0].email :
    var.workflows_service_account) :
  "${local.gcp_project_number}-compute@developer.gserviceaccount.com")
}

module "googleapi" {
  source = "./modules/googleapi"
  gcp_project_id = local.gcp_project_id
  required_apis = [
    "cloudbuild.googleapis.com",
    "artifactregistry.googleapis.com",
    "dataflow.googleapis.com",
    "workflows.googleapis.com",
  ]
}

module "storage" {
  source = "./modules/storage"
  gcp_project_id = local.gcp_project_id
  region = var.region
  gcs_bucket_name = var.gcs_bucket_name
  gcs_temp_folder = local.gcs_temp_folder
}

module "artifact_registry" {
  source = "./modules/artifact_registry"
  gcp_project_id = local.gcp_project_id
  region = var.region

  repository_name = var.repository_name
}

module "iam" {
  source = "./modules/iam"
  gcp_project_id = local.gcp_project_id
  gcp_project_number = local.gcp_project_number
  iam_members =[
    {
      role: "roles/storage.objectCreator"
      principals: [
        "serviceAccount:${local.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    {
      role: "roles/workflows.editor"
      principals: [
        "serviceAccount:${local.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    {
      role: "roles/iam.serviceAccountUser"
      principals: [
        "serviceAccount:${local.gcp_project_number}@cloudbuild.gserviceaccount.com",
        "serviceAccount:${local.workflows_service_account}"
      ]
    },
  ]
}

module "network" {
  source = "./modules/network"
  create_network = true
  gcp_project_id = local.gcp_project_id
  network_name = "vpc-dataflow"
  subnet = ""
  primary_subnet_cidr = "10.128.0.0/20"
  region = local.region
  create_nat = true
}

#module "dataflow" {
#  source = "./modules/dataflow"
#  gcp_project_id = var.gcp_project_id
#  dataflow_job_name = var.dataflow_job_name
#  gcs_bucket_name = var.gcs_bucket_name
#  flex_template_image = "${var.region}-docker.pkg.dev/${var.gcp_project_id}/${var.repository_name}/${var.flextemplate_image_name}"
#  region = var.region
#}