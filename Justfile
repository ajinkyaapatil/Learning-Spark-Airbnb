#!/usr/bin/env just --justfile

default:
  just --list

upgrade:
  uv lock --upgrade

run target:
  .venv/bin/python -m jobs.{{target}}

