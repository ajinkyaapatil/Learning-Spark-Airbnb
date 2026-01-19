#!/usr/bin/env just --justfile

default:
    just --list

upgrade:
    uv lock --upgrade

run target:
    uv run -m jobs.{{ target }}
