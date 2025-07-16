#!/usr/bin/env bash
#


# Run `taplo format` with flag `--check` in dry run to check formatting
# without overwritng the file. If any error occur, you may want to
# rerun `taplo format` to fix the formatting automatically.
set -ex
taplo format --check
