# Contributing to Multi-Source Data Integration Pipeline

Thank you for your interest in contributing! This document provides guidelines for contributing to this project.

## How to Contribute

### Reporting Issues

If you find a bug or have a suggestion:

1. Check if the issue already exists in [GitHub Issues](../../issues)
2. If not, create a new issue with:
   - Clear title and description
   - Steps to reproduce (for bugs)
   - Expected vs actual behavior
   - Environment details (AWS region, Glue version, etc.)
   - Relevant logs or error messages

### Suggesting Enhancements

For feature requests:

1. Open an issue with the `enhancement` label
2. Describe the use case and benefits
3. Provide examples if applicable
4. Consider implementation complexity

### Pull Requests

1. **Fork the Repository**
   ```bash
   git clone https://github.com/your-username/dea-c01-glue-native-connectors-iceberg-cross-source-joins-merge-s3-lake.git
   cd dea-c01-glue-native-connectors-iceberg-cross-source-joins-merge-s3-lake
   ```

2. **Create a Branch**
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/your-bug-fix
   ```

3. **Make Changes**
   - Follow existing code style
   - Add comments for complex logic
   - Update documentation if needed
   - Test your changes thoroughly

4. **Commit Changes**
   ```bash
   git add .
   git commit -m "feat: add new feature" # or "fix: resolve bug"
   ```
   
   Use conventional commit format:
   - `feat:` for new features
   - `fix:` for bug fixes
   - `docs:` for documentation
   - `refactor:` for code refactoring
   - `test:` for adding tests
   - `chore:` for maintenance tasks

5. **Push and Create PR**
   ```bash
   git push origin feature/your-feature-name
   ```
   
   Then create a Pull Request on GitHub with:
   - Clear description of changes
   - Reference to related issues
   - Test results
   - Screenshots (if UI changes)

## Development Guidelines

### Code Style

#### Python
- Follow [PEP 8](https://pep8.org/) style guide
- Use meaningful variable names
- Add docstrings for functions
- Maximum line length: 100 characters

```python
def process_data(input_df, transformation_type):
    """
    Process input DataFrame with specified transformation.
    
    Args:
        input_df: Input PySpark DataFrame
        transformation_type: Type of transformation to apply
        
    Returns:
        Transformed DataFrame
    """
    # Implementation
    pass
```

#### Terraform
- Use descriptive resource names
- Add comments for complex configurations
- Group related resources together
- Use variables for configurable values

```hcl
# S3 bucket for data lake storage
resource "aws_s3_bucket" "data_lake" {
  bucket = var.data_lake_bucket_name
  
  tags = {
    Name        = "Data Lake Bucket"
    Environment = var.environment
  }
}
```

### Testing

#### Local Testing

1. **Python Linting**
   ```bash
   pip install pylint black
   black scripts/
   pylint scripts/*.py
   ```

2. **Terraform Validation**
   ```bash
   cd terraform
   terraform fmt
   terraform validate
   ```

3. **Terraform Plan**
   ```bash
   terraform plan
   ```

#### Documentation

- Update README.md if adding features
- Update relevant docs in `docs/` directory
- Add examples for new functionality
- Keep documentation clear and concise

### Areas for Contribution

We welcome contributions in these areas:

#### High Priority
- [ ] Additional data source connectors (MySQL, PostgreSQL, etc.)
- [ ] Enhanced error handling and retry logic
- [ ] Data quality rules and validation
- [ ] Performance optimization
- [ ] CI/CD pipeline configuration

#### Medium Priority
- [ ] Additional monitoring dashboards
- [ ] Cost optimization features
- [ ] Data lineage tracking
- [ ] Automated testing suite
- [ ] Integration with AWS Lake Formation

#### Documentation
- [ ] More detailed examples
- [ ] Video tutorials
- [ ] Architecture decision records
- [ ] Best practices guides
- [ ] Troubleshooting scenarios

## Code Review Process

All submissions require review:

1. **Automated Checks**
   - Terraform validation
   - Python linting
   - Security scanning (if configured)

2. **Manual Review**
   - Code quality and style
   - Documentation completeness
   - Test coverage
   - Performance implications
   - Security considerations

3. **Approval**
   - At least one maintainer approval required
   - All comments addressed
   - CI/CD passing (when available)

## Community Guidelines

### Be Respectful
- Use welcoming and inclusive language
- Respect differing viewpoints
- Accept constructive criticism gracefully
- Focus on what's best for the community

### Be Collaborative
- Help others learn and grow
- Share knowledge and resources
- Give credit where due
- Collaborate openly

### Be Professional
- Keep discussions on-topic
- Avoid personal attacks
- Provide constructive feedback
- Maintain professional conduct

## Development Setup

### Prerequisites
```bash
# Install required tools
brew install terraform  # macOS
# or
sudo apt-get install terraform  # Linux

# Install Python dependencies
pip install -r requirements.txt

# Configure AWS CLI
aws configure
```

### Local Development

```bash
# Clone repository
git clone <repo-url>
cd dea-c01-glue-native-connectors-iceberg-cross-source-joins-merge-s3-lake

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Run tests (when available)
pytest tests/
```

### Testing Terraform Changes

```bash
cd terraform

# Format code
terraform fmt -recursive

# Validate configuration
terraform validate

# Plan changes
terraform plan -var-file=terraform.tfvars

# Apply to dev environment
terraform workspace select dev
terraform apply
```

## Release Process

Maintainers follow this process for releases:

1. Update version numbers
2. Update CHANGELOG.md
3. Create release branch
4. Test thoroughly
5. Merge to main
6. Tag release
7. Update documentation

## Questions?

- Open a [Discussion](../../discussions) for general questions
- Check [Documentation](docs/) for guides
- Review [Issues](../../issues) for known problems
- Contact maintainers for urgent matters

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

## Thank You!

Your contributions make this project better for everyone. We appreciate your time and effort! 🙏
