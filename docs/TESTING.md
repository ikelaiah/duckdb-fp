# Testing Guide

This document describes how to run and maintain the test suite for the DuckDB for FreePascal Library.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Running Tests](#running-tests)
- [Test Structure](#test-structure)
- [Writing Tests](#writing-tests)
- [Common Issues](#common-issues)
- [Troubleshooting](#troubleshooting)

## Pre-requisites

- Lazarus 3.6 or higher
- FPC (Free Pascal Compiler) version 3.2.2 or higher, which includes `fpcunit` package. 
- DuckDB library in `src/` folder, on the same level as `tests/`.

## Compilation

From the `tests/` directory:

```bash
/path/to/Lazarus/lazbuild.exe DuckDB.FP.Tests.lpi
```

## Running Tests

### Full Test Suite

From the `tests/` directory:  

```bash
./DuckDB.FP.Tests.exe -a --format=plain
```

> **Note**
> - `-a` = run all tests
> - `--format=plain` = output in plain text
> - `--format=xml` = output in XML format

Run a specific test case

```bash
./DuckDB.FP.Tests.exe --suite=TestDistinct --format=plain
```

Run all tests in a specific test suite

```bash
./DuckDB.FP.Tests.exe --suite=TDuckDBDataFrameTest --format=plain
```


### Test Output
The test runner will output:

- Test progress
- Failed test details
- Test execution time
- Total tests run/passed/failed

## Test Structure

### Main Test Runner

- `tests/DuckDB.FP.Tests.pas`: Main test suite

### Test Suites

- `DuckDB.FreePascal.Cases.pas`
  - Basic DataFrame operations
  - Data type conversions
  - Date/Time handling
  - CSV import/export
  - Column operations

### Test Data

- Test data is created in temporary directories
- Automatically cleaned up after tests complete
- Located in `FTempDir` during test execution

## Writing Tests

### Test Suites and Cases Template

> **Note**
> - Test Case = individual test  
> - Test Suite = collection of test cases

```pascal
type
  TNewTestSuite = class(TTestCase)
  private
    // Private declarations here, including helper methods
  protected
    // Protected declarations here
    procedure SetUp; override;
    procedure TearDown; override;
  published
    // Add tests here
    procedure TestNewFeature;
    // Add more tests here
  end;

implementation  

// Implement Setup and TearDown here

procedure TNewTestSuite.TestNewFeature;
var
  Frame: TDuckFrame;
begin
  Frame := CreateSampleFrame;
  try
    // Your test code here
    AssertEquals('Description', Expected, Actual);
  finally
    Frame.Free;
  end;
end;

// Add more implementation of tests here

initialization
  // Register the test suite
  // If the test suite is in the same file, register it after TDuckDBDataFrameTest
  RegisterTest(TDuckDBDataFrameTest); 
  RegisterTest(TNewTestSuite);
end.
```

### Best Practices

1. Always use try-finally blocks
2. Clean up resources (files, memory)
3. Use meaningful test names
4. Add comments explaining complex test scenarios
5. One assertion per test when possible

## Common Issues

### Memory Leaks

- Always free TDuckFrame instances
- Use `try-finally` blocks
- Clean up temporary files

### Type Conversion

- Be careful with date/time conversions
- Handle null values appropriately
- Check variant type conversions

### File Handling

- Use `FTempDir` for temporary files
- Clean up in `TearDown`
- Handle file path differences between OS

## Troubleshooting

### Common Error Messages

1. "Column not found"
   - Check column name case sensitivity
   - Verify column exists in frame

2. "Invalid type conversion"
   - Check data types match
   - Verify variant type handling

3. "Memory leak detected"
   - Look for missing Free calls
   - Check try-finally blocks

### Getting Help

- Check existing test cases for examples
- Open an issue on GitHub

## Contributing

- Write tests for new features
- Update tests for modified features
- Ensure all tests pass before submitting PR
- Add test cases for bug fixes

## Current Output

### Summary

```
Number of run tests: 42
Number of errors:    10
Number of failures:  0
```

### Expected Errors

```
List of errors:
  Error:
    Message:           TDuckDBDataFrameTest.TestCreateBlankMismatchedArrays: Column names and types arrays must have same length
    Exception class:   EDuckDBError
    Exception message: Column names and types arrays must have same length
        at   $0000000100063922

  Error:
    Message:           TDuckDBDataFrameTest.TestAddColumn: Column NewColumn already exists
    Exception class:   EDuckDBError
    Exception message: Column NewColumn already exists
        at   $0000000100063A86

  Error:
    Message:           TDuckDBDataFrameTest.TestAddRow: Number of values must match number of columns
    Exception class:   EDuckDBError
    Exception message: Number of values must match number of columns
        at   $0000000100063C3E

  Error:
    Message:           TDuckDBDataFrameTest.TestGetColumnByName: Column "NonExistentColumn" not found
    Exception class:   EDuckDBError
    Exception message: Column "NonExistentColumn" not found
        at   $0000000100056EB6

  Error:
    Message:           TDuckDBDataFrameTest.TestValuesByName: Row index out of range
    Exception class:   EDuckDBError
    Exception message: Row index out of range
        at   $0000000100056F5E

  Error:
    Message:           TDuckDBDataFrameTest.TestValues: Row index out of range
    Exception class:   EDuckDBError
    Exception message: Row index out of range
        at   $0000000100056F5E

  Error:
    Message:           TDuckDBDataFrameTest.TestInvalidColumnAccess: Column "NonExistentColumn" not found
    Exception class:   EDuckDBError
    Exception message: Column "NonExistentColumn" not found
        at   $000000010005704D

  Error:
    Message:           TDuckDBDataFrameTest.TestInvalidRowAccess: Row index out of range
    Exception class:   EDuckDBError
    Exception message: Row index out of range
        at   $0000000100056F5E

  Error:
    Message:           TDuckDBDataFrameTest.TestInvalidCSVFile: File not found: nonexistent.csv
    Exception class:   EDuckDBError
    Exception message: File not found: nonexistent.csv
        at   $0000000100063630

  Error:
    Message:           TDuckDBDataFrameTest.TestCreateFromParquetErrors: File not found: nonexistent.parquet
    Exception class:   EDuckDBError
    Exception message: File not found: nonexistent.parquet
        at   $0000000100064354la
```

---

Last updated: 2024-11-12