locals {
  subnet_name           = coalesce(var.subnet, "${var.network_name}-${var.region}")
}
resource "google_compute_network" "vpc" {
  count = var.create_network == true ? 1 : 0

  project                 = var.gcp_project_id
  name                    = var.network_name
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  count = var.create_network == true ? 1 : 0

  project                  = var.gcp_project_id
  name                     = local.subnet_name
  ip_cidr_range            = var.primary_subnet_cidr
  region                   = var.region
  network                  = google_compute_network.vpc[count.index].id
  private_ip_google_access = true
}

resource "google_compute_address" "nat_gateway" {
  count = var.create_nat ? 1 : 0

  name   = "nat-gateway"
  region = var.region
}

resource "google_compute_router" "cloud_router" {
  count = var.create_nat ? 1 : 0

  name    = "router"
  region = var.region
  network = google_compute_network.vpc.*.id[0]
}

resource "google_compute_router_nat" "router-nat" {
  count = var.create_nat ? 1 : 0

  name   = "router-nat"
  router = google_compute_router.cloud_router[count.index].name
  region = var.region
  nat_ip_allocate_option = "MANUAL_ONLY"
  nat_ips = [google_compute_address.nat_gateway[count.index].self_link]
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
  log_config {
    enable = true
    filter = "ALL"
  }
}