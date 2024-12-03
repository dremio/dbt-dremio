#!/bin/bash
set -e

mkdir -p reports

echo "Starting tests..."

test_dirs=$(find tests/ -type f \( -name 'test_*.py' -o -name '*_test.py' \) -exec dirname {} \; | sort -u)

echo "Test directories found:"
echo "$test_dirs"

# Run tests in each directory and save reports
for dir in $test_dirs; do
  echo "Running tests in directory: $dir"
  
  # Generate a safe report filename
  report_file="reports/$(echo "$dir" | tr '/' '_').txt"
  
  echo "Saving report to: $report_file"
  
  pytest "$dir" | tee "$report_file"
done

echo "All tests executed successfully."