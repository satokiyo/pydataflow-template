output "gcp_project_id" {
    value = local.gcp_project_id
}

output "gcp_project_number" {
    value = local.gcp_project_number
}

output "region" {
    value = local.region
}

output "gcs_bucket_name" {
    value = module.storage.gcs_bucket_name
}

output "gcs_bucket_url" {
    value = module.storage.gcs_bucket_url
}

output "gcs_temp_folder_name" {
    value = module.storage.gcs_temp_folder_name
}

output "gcs_temp_folder_url" {
    value = join("/", [module.storage.gcs_bucket_url,module.storage.gcs_temp_folder_name])
}

output "vpc_names" {
    value = module.network.vpc_names
}
output "vpc_gateway_ipv4s" {
    value =module.network.vpc_gateway_ipv4s
}

output "vpc_self_links" {
    value =module.network.vpc_self_links
}

output "subnet_gateway_addresses" {
    value = module.network.subnet_gateway_addresses
}

output "subnet_self_links" {
    value = module.network.subnet_self_links
}

output "subnet_ip_cidr_ranges" {
    value = module.network.subnet_ip_cidr_ranges

}
output "subnet_names" {
    value = module.network.subnet_names
}

output "repository_id" {
    value = module.artifact_registry.repository_id
}

output "repository_name" {
    value = module.artifact_registry.repository_name
}


output "iam" {
    value = [
        for value in module.iam.iam : {
            member = value.member
            role   = value.role
        }
    ]
}


#output "iam" {
#    value = {
#        for iam in "${module.iam.iam}" :
#            iam.member => [
#            for value in "${module.iam.iam}" :
#                value.role if iam.member == value.member
#        ]...
#    }
#}

output "dataflow_worker_service_account" {
    value = local.dataflow_worker_service_account
}

output "workflows_service_account" {
    value = local.workflows_service_account
}
