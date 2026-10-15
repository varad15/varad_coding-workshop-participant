resource "aws_s3_bucket" "hot_reload" {
  count         = data.aws_caller_identity.this.id == "000000000000" ? 1 : 0
  bucket        = "hot-reload"
  force_destroy = true

  lifecycle {
    prevent_destroy = false
  }

  tags = local.app_tags
}

resource "aws_s3_bucket" "this" {
  bucket        = format("%s-website-%s", var.aws_project, local.app_id)
  force_destroy = true

  lifecycle {
    prevent_destroy = false
  }

  tags = local.app_tags
}

# Data lake bucket for the medallion pipeline (landing / bronze / silver /
# gold / quarantine / _meta). Name matches the participant IAM policy
# pattern coding-workshop*<participant-id>*.
resource "aws_s3_bucket" "datalake" {
  bucket        = format("%s-datalake-%s", var.aws_project, local.app_id)
  force_destroy = true

  lifecycle {
    prevent_destroy = false
  }

  tags = local.app_tags
}

# Data lake objects are private and encrypted at rest.
resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket                  = aws_s3_bucket.datalake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_ownership_controls" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_acl" "this" {
  bucket = aws_s3_bucket.this.id
  acl    = "private"

  depends_on = [aws_s3_bucket_ownership_controls.this]
}

resource "aws_s3_bucket_website_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  index_document {
    suffix = "index.html"
  }

  error_document {
    key = "index.html"
  }
}
