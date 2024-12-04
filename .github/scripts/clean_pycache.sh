#!/bin/bash
set -e

echo "Cleaning up __pycache__ directories..."

find . -type d -name "__pycache__" -exec rm -r {} +

echo "__pycache__ directories cleaned up."