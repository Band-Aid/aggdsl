#!/bin/bash
# Validate all DSL files in the given directory

cd "$(dirname "$0")"

if [ -z "$1" ]; then
    echo "Usage: $0 <directory>"
    exit 1
fi

total=0
passed=0
failed=0

echo "Validating DSL files..."
echo "========================"

while IFS= read -r dsl_file; do
    total=$((total + 1))
    if python tools/pendo/dsl_compile.py "$dsl_file" --keep-now > /dev/null 2>&1; then
        passed=$((passed + 1))
        echo "✓ $dsl_file"
    else
        failed=$((failed + 1))
        echo "✗ $dsl_file"
        # Show the error
        python tools/pendo/dsl_compile.py "$dsl_file" --keep-now 2>&1 | head -5
    fi
done < <(find "$1" -name "*.dsl" -type f | sort)

echo ""
echo "========================"
echo "Total: $total"
echo "Passed: $passed"
echo "Failed: $failed"

if [ $failed -eq 0 ]; then
    echo "✓ All DSL files are valid!"
    exit 0
else
    echo "✗ Some DSL files have errors."
    exit 1
fi
