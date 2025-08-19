#!/bin/bash
set -e

echo "Comparing actual test failures with expected failures..."

# Enable globstar for recursive globbing
shopt -s globstar

# Extract actual failures from test reports
actual_failures=$(grep -E "(FAILED tests|ERROR tests)" reports/**/*.txt | awk '{print $2}' | sort)

# Read expected failures
expected_failures=$(sort .github/expected_failures.txt)

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
  echo "::warning::Expected test failures that did not occur (they passed):"
  echo "$missing_failures"
  exit_code=1
fi

if [ $exit_code -eq 0 ]; then
  echo "All failed tests are expected, and all expected failures have occurred."
else
  echo "Verification failed: There are unexpected or missing test failures."
fi

exit $exit_code
