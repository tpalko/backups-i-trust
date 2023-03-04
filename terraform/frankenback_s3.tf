terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region  = "us-east-1"
}

resource "aws_s3_bucket" "test_bucket" {
  bucket  = "frankentest"
  acl     = "private"
  versioning {
    enabled   = false 
  }
  tags = {
    Name  = "test"
  }
  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm   = "AES256"
      }
    }
  }
  lifecycle_rule {
    enabled   = true 
    id        = "deep_archive_transition"
    transition {
      days            = 0
      storage_class   = "DEEP_ARCHIVE"
    }
  }
}

resource "aws_s3_bucket" "backup_bucket" {
  bucket  = "frankenback"
  acl     = "private"
  versioning {
    enabled   = false 
  }
  tags = {
    Name  = "frankendeb storage"
  }
  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm   = "AES256"
      }
    }
  }
  lifecycle_rule {
    enabled   = true 
    id        = "deep_archive_transition"
    transition {
      days            = 0
      storage_class   = "DEEP_ARCHIVE"
    }
  }
}

resource "aws_s3_bucket" "archive_bucket" {
  bucket  = "frankenarchive"
  acl     = "private"
  versioning {
    enabled   = false 
  }
  tags = {
    Name  = "frankendeb archives"
  }
  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm   = "AES256"
      }
    }
  }
  lifecycle_rule {
    enabled   = true 
    id        = "deep_archive_transition"
    transition {
      days            = 0
      storage_class   = "DEEP_ARCHIVE"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "test_bucket_public_access_block" {
  bucket  = aws_s3_bucket.test_bucket.id
  
  block_public_acls   = true 
  block_public_policy = true
  ignore_public_acls  = true 
  restrict_public_buckets   = true 
}

resource "aws_s3_bucket_public_access_block" "backup_bucket_public_access_block" {
  bucket  = aws_s3_bucket.backup_bucket.id
  
  block_public_acls   = true 
  block_public_policy = true
  ignore_public_acls  = true 
  restrict_public_buckets   = true 
}

resource "aws_s3_bucket_public_access_block" "archive_bucket_public_access_block" {
  bucket  = aws_s3_bucket.archive_bucket.id
  
  block_public_acls   = true 
  block_public_policy = true
  ignore_public_acls  = true 
  restrict_public_buckets   = true 
}
