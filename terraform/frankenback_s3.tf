terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

provider "aws" {
  region  = "us-east-1"
  profile = "root"
}

resource "aws_s3_bucket" "test_bucket" {
  bucket  = "frankentest"  

  tags = {
    Name  = "test"
  }
}

resource "aws_s3_bucket_acl" "test_bucket_acl" {
  bucket = aws_s3_bucket.test_bucket.id 
  acl = "private"
}

resource "aws_s3_bucket_lifecycle_configuration" "test_bucket_lifecycle" {
  bucket = aws_s3_bucket.test_bucket.id
  rule {    
    id        = "deep_archive_transition"
    status = "Enabled"
    transition {
      days            = 0
      storage_class   = "DEEP_ARCHIVE"
    }
  }
}

resource "aws_s3_bucket_versioning" "test_bucket_versioning" {
  bucket = aws_s3_bucket.test_bucket.id
  versioning_configuration {
    status = "Suspended"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "test_bucket_encryption" {
  bucket = aws_s3_bucket.test_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm   = "AES256"
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

resource "aws_s3_bucket" "backup_bucket" {
  bucket  = "frankenback"
  
  tags = {
    Name  = "frankendeb storage"
  }
}

resource "aws_s3_bucket_acl" "backup_bucket_acl" {
  bucket = aws_s3_bucket.backup_bucket.id 
  acl = "private"
}

resource "aws_s3_bucket_lifecycle_configuration" "backup_bucket_lifecycle" {
  bucket = aws_s3_bucket.backup_bucket.id
  rule {    
    id        = "deep_archive_transition"
    status = "Enabled"
    transition {
      days            = 0
      storage_class   = "DEEP_ARCHIVE"
    }
  }
}

resource "aws_s3_bucket_versioning" "backup_bucket_versioning" {
  bucket = aws_s3_bucket.backup_bucket.id
  versioning_configuration {
    status = "Suspended"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "backup_bucket_encryption" {
  bucket = aws_s3_bucket.backup_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm   = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "backup_bucket_public_access_block" {
  bucket  = aws_s3_bucket.backup_bucket.id
  
  block_public_acls   = true 
  block_public_policy = true
  ignore_public_acls  = true 
  restrict_public_buckets   = true 
}

resource "aws_s3_bucket" "archive_bucket" {
  bucket  = "frankenarchive"

  tags = {
    Name  = "frankendeb archives"
  }
}

resource "aws_s3_bucket_acl" "archive_bucket_acl" {
  bucket = aws_s3_bucket.archive_bucket.id 
  acl = "private"
}

resource "aws_s3_bucket_lifecycle_configuration" "archive_bucket_lifecycle" {
  bucket = aws_s3_bucket.archive_bucket.id
  rule {    
    id        = "initial glacier (flexible) storage"
    status = "Enabled"
    transition {
      days            = 0
      storage_class   = "GLACIER"
    }
  }
  rule {    
    id        = "delayed deep archive transition"
    status = "Enabled"
    transition {
      days            = 14
      storage_class   = "DEEP_ARCHIVE"
    }
  }
}

resource "aws_s3_bucket_versioning" "archive_bucket_versioning" {
  bucket = aws_s3_bucket.archive_bucket.id
  versioning_configuration {
    status = "Suspended"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "archive_bucket_encryption" {
  bucket = aws_s3_bucket.archive_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm   = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "archive_bucket_public_access_block" {
  bucket  = aws_s3_bucket.archive_bucket.id
  
  block_public_acls   = true 
  block_public_policy = true
  ignore_public_acls  = true 
  restrict_public_buckets   = true 
}






