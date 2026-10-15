resource "aws_eks_cluster" "this" {
  count    = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? 1 : 0
  name     = format("%s-%s", var.aws_project, local.app_id)
  version  = "1.36"
  role_arn = local.eks_role_arn

  access_config {
    authentication_mode                         = "API_AND_CONFIG_MAP"
    bootstrap_cluster_creator_admin_permissions = true
  }

  vpc_config {
    endpoint_public_access  = true
    endpoint_private_access = true
    subnet_ids              = local.public_subnet_ids
    security_group_ids      = data.aws_security_groups.this.ids
  }

  tags = local.app_tags
}

resource "aws_eks_node_group" "this" {
  count           = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? 1 : 0
  cluster_name    = one(aws_eks_cluster.this.*.name)
  node_group_name = format("%s-%s", var.aws_project, local.app_id)
  node_role_arn   = local.eks_role_arn
  subnet_ids      = local.public_subnet_ids
  capacity_type   = var.aws_eks_type
  instance_types  = ["t3.medium", "t3a.medium", "t2.medium"]
  disk_size       = 50

  scaling_config {
    desired_size = 1
    max_size     = 1
    min_size     = 1
  }

  update_config {
    max_unavailable = 1
  }

  tags = local.app_tags
}

resource "null_resource" "helm_chart" {
  count = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? 1 : 0

  triggers = {
    cluster_id  = one(aws_eks_node_group.this.*.id)
    source_hash = local.helm_hash
    aws_ds_ip   = var.aws_ds_ip
  }

  provisioner "local-exec" {
    command = <<-EOT
      if [ -z "${var.aws_ds_ip}" ] || [ "${var.aws_ds_ip}" = "null" ]; then echo "ERROR: aws_ds_ip is not set"; exit 1; fi && \
      aws eks update-kubeconfig --region ${data.aws_region.this.region} --name ${one(aws_eks_cluster.this.*.name)} || true && \
      helm repo add jupyterhub https://hub.jupyter.org/helm-chart/ && helm repo update && \
      helm upgrade --cleanup-on-fail \
        --install jupyterhub jupyterhub/jupyterhub \
        --set proxy.secretToken="${sha256(random_pet.this.id)}" \
        --set hub.config.LDAPAuthenticator.server_address="${var.aws_ds_ip}" \
        --set-string "hub.config.LDAPAuthenticator.allowed_groups[0]=CN=${local.app_id}\,OU=Users\,OU=CORP\,DC=corp\,DC=codingworkshop\,DC=net" \
        --namespace default --values ./helm/config.yaml
    EOT
  }
}

# Publishes the sample dataset (backend/_source) to the data lake landing zone
# before any data job starts: landing/learning_activity/ is the S3 source
# system, landing/postgres_seed/ seeds Aurora. Idempotent (size-only sync).
resource "null_resource" "datalake_landing" {
  count = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? 1 : 0

  triggers = {
    bucket      = aws_s3_bucket.datalake.bucket
    source_hash = timestamp()
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      SRC="${abspath(path.module)}/../backend/_source"
      if [ -d "$SRC" ]; then
        aws s3 sync "$SRC/learning_activity" "s3://${aws_s3_bucket.datalake.bucket}/landing/learning_activity/" \
          --size-only --exclude "*" --include "*.jsonl"
        aws s3 sync "$SRC" "s3://${aws_s3_bucket.datalake.bucket}/landing/postgres_seed/" \
          --size-only --exclude "*" --include "*.csv" --include "schema.sql"
      fi
    EOT
  }
}

resource "null_resource" "helm_python_job" {
  for_each = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? local.data_names_python : {}

  triggers = {
    cluster_id  = one(aws_eks_node_group.this.*.id)
    source_hash = timestamp()
  }

  # Packages the whole job folder (job.py + its modules; tests, notebooks and
  # docs excluded) so multi-module jobs work, and passes the same environment
  # variables Lambda receives. Passwords go to a Kubernetes Secret.
  provisioner "local-exec" {
    command = <<-EOT
      set -e
      BUILD_DIR="${abspath(path.module)}/builds/${each.key}"
      mkdir -p "$BUILD_DIR"
      tar -czf "$BUILD_DIR/bundle.tgz" -C "${each.value.path}" \
        --exclude=tests --exclude=notebooks --exclude=docs \
        --exclude=__pycache__ --exclude=.pytest_cache --exclude=htmlcov \
        --exclude=.venv --exclude=venv --exclude=spark-warehouse --exclude="*.pyc" .
      base64 -w0 "$BUILD_DIR/bundle.tgz" > "$BUILD_DIR/bundle.b64"
      cat > "$BUILD_DIR/env.json" <<'JSON'
      ${jsonencode(local.job_env_vars)}
      JSON
      cat > "$BUILD_DIR/secret.json" <<'JSON'
      ${jsonencode(local.job_secret_env_vars)}
      JSON
      helm upgrade --cleanup-on-fail \
        --install ${replace(each.key, "_", "-")}-python-job ./helm/ \
        --set pythonJob.enabled=true \
        --set pythonJob.name=${replace(each.key, "_", "-")} \
        --set-file pythonJob.scriptContent=${each.value.path}/${each.value.file} \
        --set-file pythonJob.requirementsContent=${each.value.path}/${each.value.reqs} \
        --set-file pythonJob.bundleContent="$BUILD_DIR/bundle.b64" \
        --set-json pythonJob.env="$(cat "$BUILD_DIR/env.json")" \
        --set-json pythonJob.secretEnv="$(cat "$BUILD_DIR/secret.json")" \
        --namespace default
    EOT
  }

  depends_on = [null_resource.helm_chart, null_resource.datalake_landing]
}
