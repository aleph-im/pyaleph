# Project Guidelines for Claude

## Alembic Migrations

### Revision ID Format
- **ALWAYS use hexadecimal revision IDs for Alembic migrations**
- Valid characters: `0-9` and `a-f` only
- Example of valid hex ID: `a3b4c5d6e7f8`
- Example of invalid ID: `g3h4i5j6k7l8` (contains g, h, i, j, k, l)
- When creating new migrations, generate a 12-character hex string using only valid hex characters
- The revision ID must match the filename format: `NNNN_<hex_id>_<description>.py`
