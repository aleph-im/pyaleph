# Project Guidelines for Claude

## Alembic Migrations

### Revision ID Format
- **ALWAYS use hexadecimal revision IDs for Alembic migrations**
- Valid characters: `0-9` and `a-f` only
- Example of valid hex ID: `a3b4c5d6e7f8`
- Example of invalid ID: `g3h4i5j6k7l8` (contains g, h, i, j, k, l)
- When creating new migrations, generate a 12-character hex string using only valid hex characters
- The revision ID must match the filename format: `NNNN_<hex_id>_<description>.py`

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
