"""Row counts of the tables the migrations seed on a fresh schema.

Shared by the tests that assert on a freshly migrated database so that a
migration adding a seeded row is updated in one place instead of several.
"""

EXPECTED_ERROR_CODE_ROWS = 25
# balance, credit_balance, metrics_partition and credit_fold (migration 0069).
EXPECTED_CRON_JOB_ROWS = 4
# The credit_fold_state singleton (migration 0069).
EXPECTED_CREDIT_FOLD_STATE_ROWS = 1
