data "aws_caller_identity" "current" {}

# Latest Ubuntu 24.04 LTS image, published by Canonical in SSM Parameter Store.
data "aws_ssm_parameter" "ubuntu" {
  name = "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

# ------------------------------------------------------------------ data lake bucket
resource "aws_s3_bucket" "lake" {
  bucket        = "riskcoin-lake-${data.aws_caller_identity.current.account_id}-${var.region}"
  force_destroy = true # lets `terraform destroy` delete the bucket even when it has data
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ------------------------------------------------------------------ permissions for the server
resource "aws_iam_role" "server" {
  name = "riskcoin-server"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Lets you open a shell or forward ports with AWS Session Manager: no SSH, no open ports.
resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.server.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "server" {
  name = "riskcoin-server"
  role = aws_iam_role.server.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListLakeBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = aws_s3_bucket.lake.arn
      },
      {
        Sid      = "ReadWriteLakeObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "${aws_s3_bucket.lake.arn}/*"
      },
      {
        Sid      = "ReadRiskcoinSecrets"
        Effect   = "Allow"
        Action   = ["ssm:GetParameter"]
        Resource = "arn:aws:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter/riskcoin/*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "server" {
  name = "riskcoin-server"
  role = aws_iam_role.server.name
}

# ------------------------------------------------------------------ network: nothing gets in
resource "aws_security_group" "server" {
  name        = "riskcoin-server"
  description = "RiskCoin server: no inbound access, outbound to the internet"
  vpc_id      = data.aws_vpc.default.id

  egress {
    description = "CoinGecko, package downloads, S3, Session Manager"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ------------------------------------------------------------------ the server
resource "aws_instance" "server" {
  ami                         = data.aws_ssm_parameter.ubuntu.value
  instance_type               = var.instance_type
  subnet_id                   = data.aws_subnets.default.ids[0]
  vpc_security_group_ids      = [aws_security_group.server.id]
  iam_instance_profile        = aws_iam_instance_profile.server.name
  associate_public_ip_address = true

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    repo_url    = var.repo_url
    repo_branch = var.repo_branch
    region      = var.region
    lake_bucket = aws_s3_bucket.lake.bucket
  })
  user_data_replace_on_change = true

  # "standard" CPU credits: when the burst budget runs out the instance slows down
  # instead of billing extra ("unlimited" is the default for t3).
  credit_specification {
    cpu_credits = "standard"
  }

  metadata_options {
    http_tokens                 = "required" # IMDSv2 only
    http_put_response_hop_limit = 2          # containers are one network hop further away
  }

  root_block_device {
    volume_size = var.root_volume_gb
    volume_type = "gp3"
    encrypted   = true
  }

  tags = {
    Name = "riskcoin-server"
  }
}

# ------------------------------------------------------------------ cost alarm
resource "aws_budgets_budget" "monthly" {
  name         = "riskcoin-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}
