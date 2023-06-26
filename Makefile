WORKDIR:=$(shell pwd)

# build dataflow flex template
GCP_PROJECT_ID=$(shell terraform -chdir=./terraform output -raw gcp_project_id)
REGION=$(shell terraform -chdir=./terraform output -raw region)
GCS_BUCKET_NAME=$(shell terraform -chdir=./terraform output -raw gcs_bucket_name)
REPOSITORY_NAME=$(shell terraform -chdir=./terraform output -raw repository_name)
REPOSITORY_BASE:=${REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${REPOSITORY_NAME}
METADATA_FILE:=spec/metadata.json
GCS_PATH_CONTAINER_SPEC:=gs://${GCS_BUCKET_NAME}/templates/dataflow_template_spec.json
BEAM_VERSION:=2.48
WORKER_IMAGE_NAME:=pydataflow-worker
WORKER_IMAGE:=${REPOSITORY_BASE}/${WORKER_IMAGE_NAME}
FLEXTEMPLATE_IMAGE_NAME:=pydataflow-flextemplate
FLEXTEMPLATE_IMAGE:=${REPOSITORY_BASE}/${FLEXTEMPLATE_IMAGE_NAME}

# build workflows
WORKFLOW_NAME:=invoke-pydataflow-template
WORKFLOW_CONFIG_YAML:=workflow/invoke-pydataflow-template.yaml
WORKFLOW_SERVICE_ACCOUNT=$(shell terraform -chdir=./terraform output -raw workflows_service_account)

# Dataflow run settings
# Pass the path of the config Json file as a runtime argument config. (Relative path from this directory)
# example: config=configs/pipeline/<YOUR_CONFIG.json>
config:=

# Pass the path of the profile JSON file containing the connection information to the databases as a runtime argument profile or specify it here.
# example: profile=<.YOUR_PROFILES.json>
profile:=.test_connections.json

# Run dataflow via workflows
NUM_WORKERS:=1
MAX_WORKERS:=10
MACHINE_TYPE:=e2-standard-2
POLLING_SECONDS:=180
GCS_TEMP_FOLDER=$(shell terraform -chdir=./terraform output -raw gcs_temp_folder_url)
VPC_NETWORK=$(shell terraform -chdir=./terraform output -json vpc_self_links | jq -r '.[0]')
SUBNETWORK=$(shell terraform -chdir=./terraform output -json subnet_self_links | jq -r '.[0]')

# filter condition for unit/integration tests
filter:=
# docker service name launched during integration test
service:=

ifdef config
	GCS_PATH_JOB_CONFIG_JSON=gs://${GCS_BUCKET_NAME}/${config}
	LOCAL_PATH_JOB_CONFIG_JSON=${WORKDIR}/${config}
endif

ifdef profile
	GCS_PATH_CONNECTION_PROFILES_JSON=gs://${GCS_BUCKET_NAME}/${profile}
	LOCAL_PATH_CONNECTION_PROFILES_JSON=${WORKDIR}/${profile}
else
	GCS_PATH_CONNECTION_PROFILES_JSON=
	LOCAL_PATH_CONNECTION_PROFILES_JSON=
endif

.PHONY: build_all
build_all: build_dataflow build_workflow ## build and deploy dataflow and workflow ## make build_all
build_dataflow: set-project
build_workflow: set-project
run_workflow: validate-vars-config sync_config_to_storage
run_local: validate-vars-config sync_config_to_storage
sync_config_to_storage: set-project

.PHONY: build_dataflow
build_dataflow: ## build and deploy dataflow ## make build_dataflow
	gcloud builds submit \
		--config cloudbuild/cloudbuild_dataflow.yaml \
		--substitutions=_PROJECT_ID=${GCP_PROJECT_ID},_FLEXTEMPLATE_IMAGE=${FLEXTEMPLATE_IMAGE},_TEMPLATE_PATH=${GCS_PATH_CONTAINER_SPEC},_WORKER_IMAGE=${WORKER_IMAGE},_METADATA_FILE=${METADATA_FILE},_BEAM_VERSION=${BEAM_VERSION}

.PHONY: build_workflow
build_workflow: ## build and deploy workflow ## make build_workflow
	gcloud builds submit \
		--config cloudbuild/cloudbuild_workflow.yaml \
		--substitutions=_LOCATION=${REGION},_WORKFLOW_NAME=${WORKFLOW_NAME},_WORKFLOW_CONFIG_YAML=${WORKFLOW_CONFIG_YAML},_WORKFLOW_SERVICE_ACCOUNT=${WORKFLOW_SERVICE_ACCOUNT}

.PHONY: validate-vars-config
validate-vars-config:
ifndef config
	$(error config not specified!! Command argument expected: config)
endif

.PHONY: run_workflow
run_workflow: ## run beam job via workflow in dataflow ## make run_workflow config=relative/path/to/config.json
	@echo start run_workflow!!
	@echo job_config: ${GCS_PATH_JOB_CONFIG_JSON}
	@echo profile: ${GCS_PATH_CONNECTION_PROFILES_JSON}
	gcloud workflows execute ${WORKFLOW_NAME} \
		--data='{"config": "${GCS_PATH_JOB_CONFIG_JSON}", "profile": "${GCS_PATH_CONNECTION_PROFILES_JSON}", "gcs_temp_folder": "${GCS_TEMP_FOLDER}", "containerSpecGcsPath": "${GCS_PATH_CONTAINER_SPEC}", "workerContainerImage": "${WORKER_IMAGE}", "numWorkers": ${NUM_WORKERS}, "maxWorkers": ${MAX_WORKERS}, "machineType": "${MACHINE_TYPE}", "subnetwork": "${SUBNETWORK}", "region": "${REGION}", "pollingSeconds": ${POLLING_SECONDS}}' \
		--location ${REGION}

.PHONY: run_local
run_local: ## run beam job in local machine using direct runner ## make run_local config=relative/path/to/config.json
	@echo start run_local!!
	@echo job_config: ${LOCAL_PATH_JOB_CONFIG_JSON}
	@echo profile: ${LOCAL_PATH_CONNECTION_PROFILES_JSON}
	@cd src/pydataflow_template/ && \
	exec poetry run python main.py \
    --direct_runner \
	--config "${LOCAL_PATH_JOB_CONFIG_JSON}" \
	--profile "${LOCAL_PATH_CONNECTION_PROFILES_JSON}" \
    --gcs_temp_folder "${GCS_TEMP_FOLDER}"

.PHONY: sync_config_to_storage
sync_config_to_storage: ## sync profile, configs/, and examples/ with GCS  ## make sync_config_to_storage
	@if [ "$(profile)" ]; then \
		gsutil cp -j json ${profile} gs://${GCS_BUCKET_NAME}/; \
	fi
	@gsutil -m -h "Cache-Contral:no-store" rsync \
		-j json -c -r -d \
		configs \
		gs://${GCS_BUCKET_NAME}/configs/
	@gsutil -m -h "Cache-Contral:no-store" rsync \
		-j json -c -r -d \
		examples \
		gs://${GCS_BUCKET_NAME}/examples/

.PHONY: set-project
set-project: ## Check project existence and create if not found. ## make check-project
	@gcloud config set project $(GCP_PROJECT_ID)

.PHONY: unit-test
unit-test: ## unit test ## make unit-test
	@if [ -n "$(filter)" ]; then \
		poetry run python -m pytest -m unit -k "$(filter)"; \
	else \
		poetry run python -m pytest -m unit; \
	fi

.PHONY: integration-test
integration-test: ## integration test ## make integration-test
	@$(MAKE) compose-up

	@if [ "$(filter)" ]; then \
		poetry run python -m pytest -m integration -k "${filter}"; \
	else \
		poetry run python -m pytest -m integration; \
	fi

	@$(MAKE) compose-down

.PHONY: compose-up
compose-up:
	@if [ -n "$(service)" ]; then \
		docker-compose -f tests/integration/docker-compose.yaml up -d "$(service)"; \
	else \
		docker-compose -f tests/integration/docker-compose.yaml up -d; \
	fi

	@sleep 1;

.PHONY: compose-down
compose-down:
	@docker-compose -f tests/integration/docker-compose.yaml down --volume;

.PHONY: tox
tox: ## test in multiple python versions ## make tox
	-@$(MAKE) compose-up
	@poetry run python -m tox
	-@$(MAKE) compose-down

.PHONY: lint
lint: ## lint ## make lint
	@poetry run pflake8 src/ tests/

.PHONY: format
format: ## format ## make format
	@poetry run black --diff --color .
	@poetry run black .

.PHONY: isort
isort: ## isort ## make isort
	@poetry run isort .

.PHONY: type_check
type_check: ## type_check ## make type_check
	@poetry run mypy .

.PHONY: static_test
static_test: ## static_test ## make static_test
	@echo start type_check
	-@$(MAKE) type_check
	@echo start isort
	-@$(MAKE) isort
	@echo start lint
	-@$(MAKE) lint
	@echo start format
	-@$(MAKE) format
	@echo finish

help: ## print this message
	@echo "Example operations by Makefile."
	@echo ""
	@echo "Usage: make SUB_COMMAND argument_name=argument_value"
	@echo ""
	@echo "Command list:"
	@echo ""
	@printf "\033[36m%-30s\033[0m %-50s %s\n" "[Sub command]" "[Description]" "[Example]"
	@grep -E '^[/a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | perl -pe 's%^([/a-zA-Z_-]+):.*?(##)%$$1 $$2%' | awk -F " *?## *?" '{printf "\033[36m%-30s\033[0m %-50s %s\n", $$1, $$2, $$3}'

.DEFAULT_GOAL := help