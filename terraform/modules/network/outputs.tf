output "vpc_names" {
    value       = google_compute_network.vpc.*.name
    description = "The unique name of the network"
}

output "vpc_gateway_ipv4s" {
    value       = google_compute_network.vpc.*.gateway_ipv4
    description = "The IPv4 address of the gateway"
}

output "vpc_self_links" {
    value       = google_compute_network.vpc.*.self_link
    description = "The URI of the created resource"
}

output "vpc_ids" {
    value       = google_compute_network.vpc.*.id
    description = "The id of the created resource"
}

output "subnet_gateway_addresses" {
    value       = google_compute_subnetwork.subnet.*.gateway_address
    description = "The IP address of the gateway."
}

output "subnet_self_links" {
    value       = google_compute_subnetwork.subnet.*.self_link
    description = "The URI of the created resource"
}

output "subnet_ip_cidr_ranges" {
    value       = google_compute_subnetwork.subnet.*.ip_cidr_range
    description = "The IP address range that machines in this network are assigned to, represented as a CIDR block"
}
output "subnet_names" {
    value       = google_compute_subnetwork.subnet.*.name
    description = "Subnetwork name"
}