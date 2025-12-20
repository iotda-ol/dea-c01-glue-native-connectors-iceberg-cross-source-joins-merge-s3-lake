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
