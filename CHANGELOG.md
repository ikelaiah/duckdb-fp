# Changelog

All notable changes to the DuckDB Free Pascal Wrapper project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.1] - 2025-08-02

### 🚀 Major Upgrade: DuckDB API 1.3.2 Compatibility

This release represents a significant upgrade to support the latest DuckDB C Header API version 1.3.2, with comprehensive bug fixes and improved stability.

### Added
- **New DuckDB Types Support**
  - Added `DUCKDB_TYPE_STRING_LITERAL` (37)
  - Added `DUCKDB_TYPE_INTEGER_LITERAL` (38) 
  - Added `DUCKDB_TYPE_TIME_NS` (39) for nanosecond precision time handling
- **New Data Structures**
  - Added `duckdb_time_ns` record for nanosecond precision timestamps
  - Added `duckdb_bit` record for bit data type support
  - Added `duckdb_varint` record for variable-length integer support
  - Added `duckdb_instance_cache` and `duckdb_client_context` handle types
- **Enhanced Test Documentation**
  - Added `tests/TEST_RESULTS_EXPLANATION.md` explaining test result interpretation
  - Improved test method naming with `_ShouldThrowException` suffix for clarity

### Changed
- **API Updates**
  - Updated `DUCKDB_API` macro to `DUCKDB_C_API` following DuckDB 1.3.2 standards
  - Enhanced `libduckdb.pas` with all new functions and types from DuckDB 1.3.2
  - Updated enum values and struct definitions to match latest C header
- **Improved Test Clarity**
  - Renamed all expected error test methods to include `_ShouldThrowException` suffix
  - Updated test assertions to match correct SQL join behavior (5 columns instead of 4)

### Fixed
- **Critical Join Operation Bugs**
  - Fixed missing `begin`/`end` blocks in all join functions (`PerformInnerJoin`, `PerformLeftJoin`, `PerformRightJoin`, `PerformFullJoin`)
  - Resolved data copying issues causing empty columns in join results
  - Fixed row count logic in all join functions preventing duplicate result rows
  - Added missing `Break` statements in `PerformLeftJoin`, `PerformRightJoin`, and `PerformFullJoin`
  - Corrected column indexing logic in `PerformRightJoin` and `PerformFullJoin` for unmatched rows
  - Fixed access violations in `PerformRightJoin` due to incorrect column index calculations
  - Corrected column mapping logic for proper SQL join behavior across all join types
- **Memory Access Violations**
  - Fixed `ValueCounts` function access violations by switching to string-keyed dictionary
  - Implemented safe array-based iteration instead of unsafe dictionary key iteration
  - Corrected array indexing bugs in join helper functions
  - Eliminated all access violations in join operations

### Technical Improvements
- **Test Suite Enhancements**
  - Achieved 100% test success rate (58/58 tests passing)
  - All 13 expected error tests now working correctly (100% success rate)
  - All 45 functional tests now passing (100% success rate)
  - Comprehensive test coverage for DataFrame, CSV, Parquet, joins, and statistical operations
- **Code Quality**
  - Improved error handling and exception management
  - Enhanced memory management in join operations
  - Better type safety and null handling

### Performance
- **Join Operations**
  - Optimized join algorithms with proper conditional execution
  - Improved memory allocation strategies for large datasets
  - Enhanced data copying efficiency in join results

### Developer Experience
- **Clear Test Results**
  - Test output now clearly distinguishes between actual failures and expected error tests
  - Improved debugging information for join operations
  - Better error messages and exception handling

### Migration Notes
- **Breaking Changes**: None - this release maintains backward compatibility
- **Recommended Actions**: 
  - Recompile projects using the updated wrapper
  - Review any custom join operations to benefit from improved performance
  - Update test expectations if using custom test suites

### Test Results Summary
```
Total Tests: 58
Passing: 58 (100%)
Expected Error Tests: 13 (100% working correctly)
Functional Tests: 45/45 (100%)
Remaining Issues: 0 (All issues resolved)
```

### Acknowledgments
This release represents a comprehensive upgrade effort focusing on:
- API modernization and compatibility
- Critical bug fixes in core functionality  
- Enhanced developer experience and test clarity
- Improved stability and performance

---

## [1.0.0] - Previous Release
- Initial stable release of DuckDB Free Pascal Wrapper
- Basic DataFrame functionality
- CSV and Parquet file support
- Core database operations
- Statistical functions and data analysis tools
