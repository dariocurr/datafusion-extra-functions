#!/usr/bin/env bash
#


set -ex
cargo clippy --all-targets --all-features -- -D warnings
