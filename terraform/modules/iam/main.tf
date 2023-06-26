locals {
  # flatten ensures that this local value is a flat list of objects, rather
  # than a dict of list of objects.
  iam_mapping = flatten([
    for role_principals in var.iam_members : [
      for principal in role_principals.principals : {
        role = role_principals.role
        principal = principal
      }
    ]
  ])
}

resource "google_project_iam_member" "member" {
  for_each = {
    for mapping in local.iam_mapping : "${mapping.role}.${mapping.principal}" => mapping
  }
  project = var.gcp_project_id
  role    = each.value.role
  member  = each.value.principal
}
