locals {
    member  = google_project_iam_member.member
}

output "iam" {
    description = "All attributes of the created 'iam_member' resource."
    value       = local.member
}