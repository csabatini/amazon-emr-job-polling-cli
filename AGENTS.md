# Agent Guidelines for Amazon EMR Job Polling CLI

## Build/Test Commands

Run all tests across Python versions:
```bash
tox
```

Run tests with pytest:
```bash
pytest
```

Run a single test:
```bash
pytest test/test_job_client.py::test_adds_expected_python_job -v
```

Run tests with coverage:
```bash
coverage run -m pytest
coverage report
```

Lint code:
```bash
flake8 --ignore F841 --exclude emr/templates.py emr/ test/
```

Install dependencies:
```bash
pip install -r requirements.txt
```

## Code Style Guidelines

### Python Version Compatibility
- Support Python 2.7+ and Python 3.x
- Always include `from __future__ import print_function` at the top of files
- Use `six` library for Python 2/3 compatibility when needed

### Formatting
- 4 spaces for indentation (no tabs)
- Maximum line length: 79 characters (follow PEP 8)
- Use backslashes for line continuation in long expressions
- Single quotes for strings (e.g., `'string'` not `"string"`)
- No trailing whitespace
- End files with a newline

### Imports
Order imports as follows:
1. `from __future__` imports
2. Standard library imports (json, os, sys, datetime)
3. Third-party imports (boto3, click, pytest, pytz, jinja2)
4. Local imports (from emr.module import ...)

Use absolute imports for local modules: `from emr.templates import ...`

### Naming Conventions
- Functions: `snake_case` (e.g., `handle_job_request`)
- Classes: `PascalCase` (e.g., `AWSApi`)
- Constants: `UPPER_CASE` (e.g., `VALID_RUNTIMES`)
- Variables: `snake_case`
- Private methods: prefix with underscore (e.g., `_private_method`)

### Documentation
- Use Google-style docstrings for all functions and classes
- Include Args, Returns, Raises, and Example sections where applicable
- Keep docstrings under 80 characters per line
- Add inline comments for complex logic only

### Error Handling
- Raise `ValueError` for invalid business logic conditions
- Use `emr.utils.log_assertion()` for validation with logging
- Always log exceptions before re-raising
- Use try/except blocks sparingly, catch specific exceptions

### Testing
- Use pytest as the test framework
- Use pytest-mock for mocking (mocker fixture)
- Name tests with `test_` prefix and descriptive names
- Use fixtures for test setup and shared data
- Mock AWS API calls and external dependencies
- Test both success and failure cases
- Test file: `test/test_<module>.py`

### Logging
- Use structured log messages: `'action=<action>, key=value'`
- Include environment, cluster, job context in logs
- Use logging levels appropriately (INFO for normal flow, ERROR for failures)
- Use `logging.exception()` within except blocks

### AWS/Boto3 Patterns
- Wrap AWS API calls in the `AWSApi` class in `emr/utils.py`
- Support optional AWS profile parameter
- Handle pagination implicitly where possible

### Jinja2 Templates
- Store templates in `emr/templates.py`
- Exclude templates from flake8 linting
- Use consistent variable naming with the rest of the codebase

### Constants
- Define constants in `emr/constants.py`
- Group related constants together
- Use lists for enumerated values (e.g., `VALID_RUNTIMES`)

## CI/CD

- Travis CI configuration in `.travis.yml`
- Tox environments: lint, py39, py310, py311, py312, py313, py314
- Codecov integration for coverage reporting

## Project Structure

```
emr/
  __init__.py
  job_client.py      # Main CLI entry point
  utils.py           # AWS API wrapper and utilities
  templates.py       # Jinja2 templates for AWS CLI
  constants.py       # Constants and configuration
  logging.yml        # Default logging configuration
test/
  test_job_client.py
  test_utils.py
```
