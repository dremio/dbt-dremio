#!/bin/bash
set -e

# Check if expected failures file is provided as argument
if [ $# -eq 0 ]; then
    echo "Error: Expected failures file not provided."
    exit 1
fi

expected_failures_file="$1"

# Check if the expected failures file exists
if [ ! -f "$expected_failures_file" ]; then
    echo "Error: Expected failures file '$expected_failures_file' not found."
    exit 1
fi

echo "Comparing actual test failures with expected failures from: $expected_failures_file"

# Enable globstar for recursive globbing
shopt -s globstar

# Extract actual failures from test reports
actual_failures=$(grep -E "(FAILED tests|ERROR tests)" reports/**/*.txt | awk '{print $2}' | sort -u)

# Read expected failures
expected_failures=$(sort "$expected_failures_file")

echo "Expected Failures:"
echo "$expected_failures"
echo ""
echo "Actual Failures:"
echo "$actual_failures"
echo ""

# Identify unexpected failures (in actual but not in expected)
unexpected_failures=$(comm -13 <(echo "$expected_failures") <(echo "$actual_failures"))

# Identify missing expected failures (in expected but not in actual)
missing_failures=$(comm -23 <(echo "$expected_failures") <(echo "$actual_failures"))

# Initialize exit code
exit_code=0

if [ -n "$unexpected_failures" ]; then
  echo "Unexpected test failures detected:"
  echo "$unexpected_failures"
  exit_code=1
fi

if [ -n "$missing_failures" ]; then
  echo "⚠️ Expected test failures that did not occur (they passed):"
  echo "$missing_failures"
  exit_code=1
fi

if [ $exit_code -eq 0 ]; then
  echo "All failed tests are expected, and all expected failures have occurred."
else
  echo "Verification failed: There are unexpected or missing test failures."
fi

exit $exit_code
