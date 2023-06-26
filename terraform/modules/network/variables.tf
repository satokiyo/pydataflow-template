variable "gcp_project_id" {
  type        = string
  description = "Project ID to deploy resources into"
}

variable "region" {
  type        = string
  description = "Region to deploy regional-resources into. This must match subnet's region if deploying into existing network (e.g. Shared VPC). See `subnet` parameter below"
}

variable "create_network" {
  description = "Boolean value specifying if a new network needs to be created."
  default     = false
  type        = bool
}

variable "network_name" {
  description = "Network to deploy resources into"
  type        = string
}

variable "subnet" {
  type        = string
  description = "Subnet to deploy resources into. This is required when deploying into existing network (`create_network=false`) (e.g. Shared VPC)"
  default     = ""
}

variable "primary_subnet_cidr" {
  type        = string
  description = "The CIDR Range of the primary subnet"
  default     = "10.128.0.0/20"
}

variable "create_nat" {
  type        = bool
  description = "Whether to create a nat or not"
  default     = false
}
