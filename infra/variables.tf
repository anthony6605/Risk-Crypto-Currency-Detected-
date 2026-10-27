variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "EC2 size. t3.large (8 GB) runs the whole stack except Spark."
  type        = string
  default     = "t3.large"
}

variable "repo_url" {
  description = "HTTPS URL of your public RiskCoin GitHub repository."
  type        = string
}

variable "repo_branch" {
  description = "Branch to deploy."
  type        = string
  default     = "main"
}

variable "alert_email" {
  description = "Where AWS Budgets sends cost alerts."
  type        = string
}

variable "monthly_budget_usd" {
  description = "Monthly cost (USD) that triggers budget alerts."
  type        = number
  default     = 20
}

variable "root_volume_gb" {
  description = "Disk size for Docker images, Kafka, PostgreSQL and Bronze files."
  type        = number
  default     = 30
}
