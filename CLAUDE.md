# PyAleph Development Notes

## Development Workflow

After each development task, always run these commands in order:

```bash
# 1. Format code (black, isort, ruff, etc.)
hatch run linting:fmt

# 2. Run all linting checks (ruff, black, isort, mypy, etc.)
hatch run linting:all

# 3. Run tests
hatch run testing:test tests/path/to/test.py -v
```

## Running Tests

Use hatch with the testing environment to run tests:

```bash
hatch run testing:test tests/path/to/test.py -v
```

For example:
```bash
hatch run testing:test tests/api/test_costs.py -v
```

Run all tests:
```bash
hatch run testing:test
```
