terraform {
  required_version = ">= 1.3.0"

  required_providers {
    observe = {
      source  = "terraform.observeinc.com/observeinc/observe"
      version = "~> 0.14"
    }
  }
}

provider "observe" {
  customer  = var.observe_customer_id
  api_token = var.observe_api_token
}
