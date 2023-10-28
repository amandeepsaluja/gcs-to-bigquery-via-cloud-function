
provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

terraform {
  backend "gcs" {
    bucket = "terraform-state-bucket-gcp-practice-project-aman"
    prefix = "cloud-function/process-gcs-excel-via-cloud-function"
  }
}
