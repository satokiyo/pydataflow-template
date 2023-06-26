# Architecture Overview

![](imgs/diagram.png)

# 0. Prerequisite

1. OS: Linux (Ubuntu)
1. Python (3.8-3.10)
1. Google Cloud SDK
1. Terraform
1. Docker
1. Docker Compose
1. (Optional) Java runtime: required for some modules

Setup Python virtual environment

```bash
poetry install
```

# 1. Infrastructure Setup, Build & Deployment

## 1.1. Create GCP Project and Set Authentication Credentials

```bash
gcloud projects create <PROJECT_ID> --name <PROJECT_NAME>
gcloud config set project <PROJECT_ID>
gcloud auth login
```

## 1.2. Infrastructure Setup with Terraform

Set the necessary values in terraform/terraform.tfvars.

```
gcp_project_id = "<GCP_PROJECT_ID>"
region = "asia-northeast2"
gcp_project_name = "<GCP_PROJECT_NAME>"
gcp_billing_account_id = "<GCP_BILLING_ACCOUNT_ID>"
gcs_bucket_name = "py-dataflow-bucket"
gcs_temp_folder = "pydataflow-temp/"
repository_name = "pydataflow-repo"
dataflow_worker_service_account = ""
use_externally_managed_dataflow_sa = false
workflows_service_account = ""
use_externally_managed_workflows_sa = false
```

Run the following commands:

```bash
cd terraform/
terraform init
terraform import google_project.project <PROJECT_ID>
terraform plan
terraform apply
```

## 1.3. Build and Deploy Dataflow

Create Dataflow and Workflows using the gcloud command.

Define the necessary variables in the Makefile:

| Variable                | Description                      |
| :---------------------- | :------------------------------- |
| WORKER_IMAGE_NAME       | Docker image name (worker)       |
| FLEXTEMPLATE_IMAGE_NAME | Docker image name (flextemplate) |
| BEAM_VERSION            | Apache Beam SDK version          |

Build and deploy using the make command:

```bash
make build_dataflow

#  Command executed
#  gcloud builds submit \
#    --config cloudbuild/cloudbuild_dataflow.yaml \
#    --substitutions=_PROJECT_ID=${GCP_PROJECT_ID},_FLEXTEMPLATE_IMAGE=${FLEXTEMPLATE_IMAGE},_TEMPLATE_PATH=${GCS_PATH_CONTAINER_SPEC},_WORKER_IMAGE=${WORKER_IMAGE},_METADATA_FILE=${METADATA_FILE},_BEAM_VERSION=${BEAM_VERSION}
```

## 1.4. Build and Deploy Workflows

Define the necessary variables in the Makefile:

| Variable      | Description   |
| :------------ | :------------ |
| WORKFLOW_NAME | Workflow name |

Build and deploy using the make command:

```bash
make build_workflow

#  Command executed
#  gcloud builds submit \
#    --config cloudbuild/cloudbuild_workflow.yaml \
#    --substitutions=_LOCATION=${REGION},_WORKFLOW_NAME=${WORKFLOW_NAME},_WORKFLOW_CONFIG_YAML=${WORKFLOW_CONFIG_YAML},_WORKFLOW_SERVICE_ACCOUNT=${WORKFLOW_SERVICE_ACCOUNT}
```

---

# 2. Running Jobs

## 2.1. Define JSON Configuration File

Define the configuration for the Dataflow job in a JSON configuration file.

For more details, refer to [Define Pipeline](config/README.md).

## 2.2. Executing Dataflow Jobs via Workflows

Launch Dataflow jobs through Workflows. By passing a JSON configuration file as a parameter when starting the job, you can dynamically execute the defined pipeline.

Define the necessary variables in the Makefile.

| Variable         | Description                                                                 |
| :--------------- | :-------------------------------------------------------------------------- |
| profile [1]      | JSON file path that describes the connection profile information for the DB |
| NUM_WORKERS      | Default number of workers                                                   |
| MAX_WORKERS      | Maximum number of workers                                                   |
| MACHINE_TYPE [2] | Machine type                                                                |
| POLLING_SECONDS  | Job status polling interval (in seconds)                                    |

[1] For example, pass the path as profile=.connections.json. See [here](config/module/source/mysql.md) for JSON file format.

[2]
In Apache Beam, the unit of distributed processing is the DoFn class. In the case of Python SDK, SDK processes exist per vCPU core and are shared among multiple threads. For batch processing, the maximum number of DoFn instances that can be executed per vCPU core is 1, and 1 thread executes the DoFn instances. Therefore, for batches, the number of cores equals the parallelism. If the parallelism is high (large amount of data), it is preferable to use cost-effective E2 series, which has a low cost per core. However, if 1 core is sufficient (small amount of data), n1-standard-1 (default for batches), which has a lineup starting from 1 core, would be advantageous. The machine type can be switched as needed, but the cost is mainly influenced by the amount of data, so e2-standard-2 is used as the default.

Execute the make command and specify the JSON configuration file as a command line argument.

```bash
make run_workflow config=path/to/config.json

# Executed command
# gcloud workflows execute ${WORKFLOW_NAME} \
#     --data='{"config": "${GCS_PATH_JOB_CONFIG_JSON}", "profile": "${GCS_PATH_CONNECTION_PROFILES_JSON}", "gcs_temp_folder": "${GCS_TEMP_FOLDER}", "containerSpecGcsPath": "${GCS_PATH_CONTAINER_SPEC}", "workerContainerImage": "${WORKER_IMAGE}", "numWorkers": ${NUM_WORKERS}, "maxWorkers": ${MAX_WORKERS}, "machineType": "${MACHINE_TYPE}", "subnetwork": "${SUBNETWORK}", "region": "${REGION}"}' \
#     --location ${REGION}
```

### [Note]

Please specify the path to the JSON configuration file as a **relative path**. The reason is that the `configs/` and `examples/` directories in the repository are automatically synchronized with the specified bucket in GCS. Dataflow jobs refer to the synchronized configuration JSON file in GCS to execute the jobs.

[example]

```bash
# @repository top
# OK.
make run_workflow config=configs/path/to/config.json
# NG.
make run_workflow config=gs://bucket_name/configs/path/to/config.json
```

## 2.3. Running Jobs on Local Machine (Processing Small-scale Data with Direct Runner)

The process is the same as in 2.2.

```bash
make run_local config=path/to/config.json
```

---

# 3. Scheduling Job Execution

Create triggers for periodically executing Workflows using Cloud Scheduler. When invoking Workflows, POST the JSON configuration file as a parameter to execute the defined jobs.

## 3.1. Configure the JSON Configuration File

```json
{
  "schedule": {
    "name": "scheduler-job-name",
    "cron": "0 0 * * *",
    "enable": "false"
  }
}
```

| parameter | description                           |
| :-------- | :------------------------------------ |
| name      | Specify the name of the trigger       |
| cron      | Specify the frequency in cron format  |
| enable    | Enable or disable (`true` or `false`) |

## 3.2. Create/Update/Delete Triggers with make Command

```bash
make trigger_workflow config=path/to/config.json
```

Logic for creating/updating/deleting triggers:

- enable==true && the trigger specified in the name already exists: update
- enable==true && the trigger specified in the name does not exist yet: create
- enable==false && the trigger specified in the name already exists: delete
- Otherwise: do nothing

# 4. Major make Commands

In addition to infrastructure setup and job execution commands, development commands such as testing and static analysis are also available. You can see the list of commands using the following command:

```bash
make help

# Example operations by Makefile.

# Usage: make SUB_COMMAND argument_name=argument_value

# Command list:

# [Sub command]                  [Description]                                      [Example]
# build_all                      build and deploy dataflow and workflow             make build_all
# build_dataflow                 build and deploy dataflow                          make build_dataflow
# build_workflow                 build and deploy workflow                          make build_workflow
# run_workflow                   run beam job via workflow in dataflow              make run_workflow config=relative/path/to/config.json
# run_local                      run beam job in local machine using direct runner  make run_local config=relative/path/to/config.json
# sync_config_to_storage         sync configs/ examples/ with GCS                   make sync_config_to_storage
# set-project                    Check project existence and create if not found.   make check-project
# unit-test                      unit test                                          make unit-test
# integration-test               integration test                                   make integration-test
# tox                            test in multiple python versions                   make tox
# lint                           lint                                               make lint
# format                         format                                             make format
# isort                          isort                                              make isort
# type_check                     type_check                                         make type_check
# static_test                    static_test                                        make static_test
# help                           print this message
```

# TODO

## Features

- Add modules (Data source, Transform, ML, etc.)
- Support streaming
- Support YAML format for configuration files
- Create a simple interface (e.g., using streamlit)
- Support timezone conversion

## Implementation

- Add implementation of incremental mode merge
- Rethink the places where the Dataflow job name is based on the configuration file name
- Classify primitive types and encapsulate validation logic in classes
- Replace inheritance with composition (Decorator) in certain places
- Configure CI/CD
- Change the polling of Workflow job status to callbacks from polling
- Change the SQL rewriting in incremental mode to use CTE
- Speed up integration tests
