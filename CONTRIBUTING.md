# Contributing Guide

Thank you for your interest in contributing to this project! This guide will help you understand our development process and standards.

## Code Structure Principles

### 1. Maximum Modularity
- Each module should have a single, well-defined responsibility
- Avoid tight coupling between modules
- Use dependency injection where appropriate
- Follow SOLID principles

### 2. Reusability
- Write code that can be used across multiple jobs and contexts
- Avoid hardcoding values - use configuration instead
- Create abstract base classes for common patterns
- Use factory patterns for object creation

### 3. Organization
- Keep related files together in dedicated directories
- Minimize files in the root directory
- Use clear, descriptive names for files and directories
- Maintain consistent structure across similar components

## Adding New Components

### New Connector
1. Create class inheriting from `BaseConnector`
2. Implement all abstract methods
3. Add to `ConnectionFactory.CONNECTORS`
4. Create configuration template in `config/`
5. Add example usage in `examples/`
6. Update documentation

### New Transformation
1. Add static method to `DataTransformer` or create new module
2. Include comprehensive docstrings
3. Add example usage
4. Update `src/transformations/__init__.py`

### New Utility Library
1. Create module in `lib/` subdirectory
2. Follow existing patterns (logging, validation)
3. Include unit tests
4. Update `lib/README.md`

## Code Style

### Python
- Follow PEP 8
- Use type hints where beneficial
- Maximum line length: 100 characters
- Use docstrings for all public methods

### Docstrings
```python
def method_name(param1: str, param2: int) -> bool:
    """
    Brief description of what the method does
    
    Args:
        param1: Description of param1
        param2: Description of param2
        
    Returns:
        bool: Description of return value
        
    Raises:
        ValueError: When and why this is raised
    """
```

### Naming Conventions
- Classes: `PascalCase`
- Functions/Methods: `snake_case`
- Constants: `UPPER_SNAKE_CASE`
- Private methods: `_leading_underscore`

## Documentation

### Code Documentation
- All public APIs must have docstrings
- Include usage examples in docstrings
- Document assumptions and limitations
- Explain complex algorithms

### README Files
- Every directory should have a README.md
- Explain purpose and contents
- Include usage examples
- Link to related documentation

### Comprehensive Guide
- Update step-by-step guide when adding major features
- Maintain consistency with existing style
- Include code examples
- Add to appropriate difficulty level (novice/expert)

## Testing

### Unit Tests
- Place in `tests/unit/`
- Test individual components in isolation
- Use mocks for external dependencies
- Aim for >80% code coverage

### Integration Tests
- Place in `tests/integration/`
- Test component interactions
- Use test fixtures for data
- Can use actual AWS services in test account

### Example Validation
- All examples must be runnable
- Include clear instructions
- Use realistic configurations
- Document expected outputs

## Configuration

### Configuration Files
- Store in appropriate `config/` subdirectory
- Use JSON format for structured configs
- Include comments via `_comment` fields
- Never commit secrets

### Secrets Management
- Use placeholders: `${SECRET:path/to/secret}`
- Document required secrets in README
- Provide example values (not real ones)

## Deployment

### Library Packaging
- Update version in `__init__.py` files
- Test imports after packaging
- Run deployment script to upload to S3

### Script Changes
- Make scripts executable: `chmod +x script.sh`
- Include usage instructions in script header
- Test on clean environment

## Pull Request Process

1. **Fork and Branch**
   - Fork the repository
   - Create feature branch: `feature/your-feature-name`

2. **Make Changes**
   - Follow code style guidelines
   - Add/update tests
   - Update documentation

3. **Test Locally**
   - Run validation scripts
   - Test example code
   - Check for linting errors

4. **Submit PR**
   - Clear description of changes
   - Reference related issues
   - Include screenshots if UI changes
   - Update CHANGELOG.md

5. **Review Process**
   - Address review comments
   - Keep commits clean
   - Squash if requested

## Review Checklist

Before submitting, ensure:
- [ ] Code follows style guidelines
- [ ] All tests pass
- [ ] Documentation updated
- [ ] Examples work correctly
- [ ] No secrets committed
- [ ] Configuration templates provided
- [ ] README files updated
- [ ] Changes logged in CHANGELOG.md

## Questions?

- Review existing code for patterns
- Check documentation for guidance
- Open an issue for clarification
- Reference the comprehensive guide

## License

By contributing, you agree that your contributions will be licensed under the same terms as the project.

---

Thank you for contributing to make this project better! 🎉
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
