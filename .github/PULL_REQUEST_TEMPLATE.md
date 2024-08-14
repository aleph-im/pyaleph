Explain what problem this PR is resolving

Related Clickup or Jira tickets : ALEPH-XXX

## Self proofreading checklist

- [ ] Is my code clear enough and well documented
- [ ] Are my files well typed
- [ ] New translations have been added or updated if new strings have been introduced in the frontend
- [ ] Database migrations file are included
- [ ] Are there enough tests
- [ ] Documentation has been included (for new feature)

## Changes

Explain the changes that were made. The idea is not to list exhaustively all the changes made (GitHub already provides a full diff), but to help the reviewers better understand:
- which specific file changes go together, e.g: when creating a table in the front-end, there usually is a config file that goes with it
- the reasoning behind some changes, e.g: deleted files because they are now redundant
- the behaviour to expect, e.g: tooltip has purple background color because the client likes it so, changed a key in the API response to be consistent with other endpoints

## How to test

Explain how to test your PR.
If a specific config is required explain it here (account, data entry, ...)

## Print screen / video

Upload here print screens or videos showing the changes if relevant.

## Notes

Things that the reviewers should know: known bugs that are out of the scope of the PR, other trade-offs that were made.
If the PR depends on a PR in another repo, or merges into another PR (i.o. main), it should also be mentioned here
