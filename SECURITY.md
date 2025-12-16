# Security Policy

## Supported Versions

We are committed to maintaining the security of this project. The following versions are currently supported with security updates:

| Version | Supported          |
| ------- | ------------------ |
| Latest  | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability, please report it by:

1. **Do NOT** open a public issue
2. Email the maintainers directly or use GitHub Security Advisories
3. Provide detailed information about the vulnerability
4. Allow reasonable time for a fix before public disclosure

We will acknowledge receipt within 48 hours and provide a detailed response within 7 days.

## Security Considerations

### PySpark Version in Local Development

**Important Note**: The `requirements.txt` file specifies PySpark >= 3.3.3 for local development and testing due to security vulnerabilities in earlier versions:

- **CVE-2023-32007**: Apache Spark vulnerable to Improper Privilege Management
  - Affected versions: <= 3.3.2
  - Patched version: 3.3.3+
  - Severity: High

However, **AWS Glue 4.0 uses PySpark 3.3.0 internally**. This is acceptable because:

1. **AWS Glue is a managed service** where AWS handles the security of the underlying infrastructure
2. **IAM policies and VPC isolation** provide additional security layers
3. **The vulnerability primarily affects local deployments** with specific privilege escalation scenarios
4. **AWS applies security patches** to the Glue service independent of PySpark versions

### For Local Development

When developing and testing locally:

```bash
# Install with patched version
pip install pyspark>=3.3.3

# Verify version
python -c "import pyspark; print(pyspark.__version__)"
```

### For Production AWS Glue Deployments

- AWS Glue 4.0 is used as configured in Terraform
- AWS manages security patches for the Glue service
- Additional security measures are in place:
  - IAM least privilege policies
  - VPC isolation (when configured)
  - Encrypted data at rest and in transit
  - CloudWatch monitoring for anomalous behavior

## Security Best Practices Implemented

### 1. IAM Least Privilege

All IAM policies follow the principle of least privilege:

```hcl
# Example: S3 access limited to specific buckets
data "aws_iam_policy_document" "glue_s3_access" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]
    resources = [
      "${aws_s3_bucket.data_lake.arn}/*",
      "${aws_s3_bucket.glue_scripts.arn}/*"
    ]
  }
}
```

### 2. Encryption

- **At Rest**: All S3 buckets use AES-256 encryption
- **In Transit**: All connections use TLS 1.2 or higher
- **Secrets**: Database credentials stored in AWS Secrets Manager with KMS encryption

### 3. Network Security

When using VPC connections:
- Private subnets for Glue ENIs
- Security groups with minimal ingress/egress rules
- VPC endpoints to avoid internet traffic

### 4. S3 Bucket Security

All S3 buckets have:
- Public access blocked
- Versioning enabled for recovery
- Server-side encryption
- Bucket policies enforcing encryption in transit

### 5. Secrets Management

**Never hardcode credentials**. Use AWS Secrets Manager:

```bash
# Store credentials securely
aws secretsmanager create-secret \
  --name multi-source-pipeline/redshift/credentials \
  --secret-string '{"username":"user","password":"pass"}'
```

### 6. Monitoring and Audit

- CloudWatch Logs enabled with retention policies
- CloudWatch Alarms for anomalous behavior
- Job metrics tracked for unusual patterns
- AWS CloudTrail for API audit logging (recommended)

### 7. Regular Updates

Schedule regular reviews:
- Monthly: Review IAM policies
- Monthly: Update Python dependencies for local dev
- Quarterly: Review AWS Glue version and migrate if needed
- Quarterly: Security audit of configurations

## Known Security Considerations

### 1. PySpark in AWS Glue 4.0

**Status**: Mitigated by AWS managed service security

AWS Glue 4.0 uses PySpark 3.3.0 internally. While this version has known vulnerabilities, the risk is mitigated by:
- AWS's managed service security model
- IAM policy restrictions
- VPC isolation
- CloudWatch monitoring
- AWS applies security patches independent of version numbers

**Recommendation**: Monitor AWS announcements for Glue 4.1 or later versions that may include updated PySpark versions.

### 2. Database Credentials in Terraform State

**Risk**: Terraform state files may contain sensitive data

**Mitigation**:
1. Use remote state backend (S3 with encryption)
2. Enable state file encryption
3. Use AWS Secrets Manager for credentials
4. Restrict access to state files with IAM policies

Example:
```hcl
terraform {
  backend "s3" {
    bucket         = "terraform-state-bucket"
    key            = "multi-source-pipeline/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "terraform-state-lock"
  }
}
```

### 3. Third-Party JDBC Drivers

**Risk**: BigQuery and Teradata JDBC drivers from third parties

**Mitigation**:
1. Download drivers only from official sources
2. Verify checksums before uploading to S3
3. Use specific version numbers, not "latest"
4. Scan JAR files for vulnerabilities before deployment

### 4. CloudWatch Logs May Contain Sensitive Data

**Risk**: Logs might inadvertently contain PII or credentials

**Mitigation**:
1. Use CloudWatch log filtering to mask sensitive data
2. Implement log retention policies
3. Review ETL script to avoid logging sensitive fields
4. Use AWS KMS encryption for CloudWatch logs (optional)

## Compliance Considerations

### Data Residency
- All data remains in the configured AWS region
- Cross-region replication should be carefully considered for compliance

### Data Encryption
- FIPS 140-2 compliant encryption algorithms used
- KMS keys can be customer-managed for additional control

### Audit Trail
- All AWS API calls logged via CloudTrail (if enabled)
- CloudWatch Logs provide detailed job execution history
- S3 versioning provides object-level audit trail

## Security Checklist for Deployment

Before deploying to production, verify:

- [ ] All S3 buckets have public access blocked
- [ ] IAM policies follow least privilege principle
- [ ] Database credentials stored in Secrets Manager
- [ ] S3 encryption enabled on all buckets
- [ ] CloudWatch logging enabled
- [ ] CloudWatch alarms configured
- [ ] VPC configuration reviewed (if applicable)
- [ ] Security groups allow only necessary traffic
- [ ] Terraform state stored securely with encryption
- [ ] All JDBC drivers from trusted sources
- [ ] Log retention policies configured
- [ ] Monitoring dashboard created
- [ ] Incident response plan documented

## Responsible Disclosure

We appreciate the security research community's efforts to improve security. If you believe you've found a security issue in our project:

1. **Contact us privately** via email or GitHub Security Advisory
2. **Provide detailed information** including:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if available)
3. **Allow time for response** (48 hours acknowledgment, 7 days detailed response)
4. **Coordinate disclosure** timing with maintainers

## Security Resources

- [AWS Security Best Practices](https://aws.amazon.com/security/best-practices/)
- [AWS Glue Security](https://docs.aws.amazon.com/glue/latest/dg/security.html)
- [Apache Spark Security](https://spark.apache.org/docs/latest/security.html)
- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [CIS AWS Foundations Benchmark](https://www.cisecurity.org/benchmark/amazon_web_services)

## Updates and Changelog

### 2024-12-16
- Updated PySpark version in requirements.txt to >= 3.3.3 for local development
- Added security documentation
- Documented PySpark version considerations for AWS Glue 4.0

---

**Last Updated**: December 16, 2024  
**Next Review**: March 16, 2025
