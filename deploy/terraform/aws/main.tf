###############################################################################
# RecsPlatform — AWS production topology (study case)
#
# EKS (compute) + MSK (Kafka) + DocumentDB (Mongo-compatible) +
# ElastiCache Redis (cache) + supporting VPC/IAM/S3.
###############################################################################

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.60" }
  }
  # backend "s3" {
  #   bucket = "recsplatform-tfstate"
  #   key    = "prod/terraform.tfstate"
  #   region = "us-east-1"
  # }
}

provider "aws" {
  region = var.region
}

variable "region" { default = "us-east-1" }
variable "project" { default = "recsplatform" }
variable "vpc_cidr" { default = "10.40.0.0/16" }

data "aws_availability_zones" "available" {
  state = "available"
}

# --- Networking --------------------------------------------------------------
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.8"

  name = var.project
  cidr = var.vpc_cidr

  azs             = slice(data.aws_availability_zones.available.names, 0, 3)
  public_subnets  = [for i in range(3) : cidrsubnet(var.vpc_cidr, 8, i)]
  private_subnets = [for i in range(3) : cidrsubnet(var.vpc_cidr, 8, i + 16)]

  enable_nat_gateway   = true
  single_nat_gateway   = false # one per AZ: survives an AZ failure
  enable_dns_hostnames = true
}

# --- EKS ---------------------------------------------------------------------
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.8"

  name       = "${var.project}-eks"
  cluster_version = "1.30"
  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  cluster_endpoint_public_access = true

  eks_managed_node_groups = {
    services = {
      instance_types = ["m7g.large"] # Graviton: better price/perf
      min_size       = 3
      max_size       = 30
      desired_size   = 3
    }
  }

  cluster_addons = {
    coredns    = { most_recent = true }
    kube-proxy = { most_recent = true }
    vpc-cni    = { most_recent = true }
    metrics-server = { most_recent = true } # feeds the HPA
  }
}

# --- MSK (Kafka) -------------------------------------------------------------
resource "aws_security_group" "msk" {
  name_prefix = "${var.project}-msk"
  vpc_id      = module.vpc.vpc_id
  ingress {
    description     = "Kafka from EKS nodes"
    security_groups = [module.eks.node_security_group_id]
    from_port       = 9092
    to_port         = 9092
    protocol        = "tcp"
  }
}

resource "aws_msk_cluster" "events" {
  cluster_name           = "${var.project}-events"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 3 # one per AZ
  broker_node_group_info {
    instance_type   = "kafka.m7g.large"
    client_subnets  = module.vpc.private_subnets
    security_groups = [aws_security_group.msk.id]
    storage_info {
      ebs_storage_info { volume_size = 200 }
    }
  }
  # Money- and durability-relevant defaults: in-sync replicas and min ISR.
  configuration_info {
    arn      = aws_msk_configuration.tuning.arn
    revision = aws_msk_configuration.tuning.latest_revision
  }
}

resource "aws_msk_configuration" "tuning" {
  name           = "${var.project}-tuning"
  kafka_versions = ["3.6.0"]
  server_properties = <<-EOF
    default.replication.factor=3
    min.insync.replicas=2
    unclean.leader.election.enable=false
    auto.create.topics.enable=false
  EOF
}

# --- DocumentDB (MongoDB-compatible API) --------------------------------------
resource "aws_docdb_cluster" "main" {
  cluster_identifier       = "${var.project}-docdb"
  engine                   = "docdb"
  master_username          = "recsapp"
  master_password          = var.docdb_password
  db_subnet_group_name     = aws_docdb_subnet_group.main.name
  vpc_security_group_ids   = [aws_security_group.docdb.id]
  backup_retention_period  = 7
  preferred_backup_window  = "03:00-04:00"
  storage_encrypted        = true
}

resource "aws_docdb_cluster_instance" "main" {
  count              = 3
  identifier         = "${var.project}-docdb-${count.index}"
  cluster_identifier = aws_docdb_cluster.main.id
  instance_class     = "db.r6g.large"
}

resource "aws_docdb_subnet_group" "main" {
  name       = "${var.project}-docdb"
  subnet_ids = module.vpc.private_subnets
}

resource "aws_security_group" "docdb" {
  name_prefix = "${var.project}-docdb"
  vpc_id      = module.vpc.vpc_id
  ingress {
    security_groups = [module.eks.node_security_group_id]
    from_port       = 27017
    to_port         = 27017
    protocol        = "tcp"
  }
}

variable "docdb_password" {
  sensitive = true
  type      = string
}

# --- ElastiCache (Redis) ------------------------------------------------------
resource "aws_elasticache_replication_group" "cache" {
  replication_group_id       = "${var.project}-cache"
  description                = "Recommendation cache"
  engine                     = "redis"
  engine_version             = "7.1"
  node_type                  = "cache.r7g.large"
  num_cache_clusters         = 3
  replica_spread_azs         = slice(data.aws_availability_zones.available.names, 0, 3)
  automatic_failover_enabled = true
  multi_az_enabled           = true
  subnet_group_name          = aws_elasticache_subnet_group.cache.name
  security_group_ids         = [aws_security_group.redis.id]
  at_rest_encryption_enabled = true
}

resource "aws_elasticache_subnet_group" "cache" {
  name       = "${var.project}-cache"
  subnet_ids = module.vpc.private_subnets
}

resource "aws_security_group" "redis" {
  name_prefix = "${var.project}-redis"
  vpc_id      = module.vpc.vpc_id
  ingress {
    security_groups = [module.eks.node_security_group_id]
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
  }
}

# --- Observability bucket (e.g. Loki/audit archives) ---------------------------
resource "aws_s3_bucket" "logs" {
  bucket = "${var.project}-logs-${data.aws_caller_identity.current.account_id}"
}

data "aws_caller_identity" "current" {}

output "eks_kubeconfig_cmd" {
  value       = "aws eks update-kubeconfig --name ${module.eks.cluster_name} --region ${var.region}"
  description = "Configure kubectl for the cluster."
}
