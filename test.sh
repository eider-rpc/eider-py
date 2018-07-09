#!/bin/bash -ex

venv/bin/flake8
venv/bin/pytest --cov-report=term --cov-report=html --cov=eider
