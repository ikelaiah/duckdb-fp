# Changelog

All notable changes to the DuckDB Free Pascal Wrapper project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.5.5.1] - 2026-09-21

### 📚 Documentation, CI, and Tooling

A maintenance release with no library code changes: documentation is now accurate, continuous integration has been added, and all examples can be built with one command.

### Added
- **Continuous integration** (`.github/workflows/ci.yml`)
  - Builds the Lazarus package and the test suite, runs the tests, and builds all examples on Windows
  - Runs against Lazarus 4.0 (documented minimum) and the current stable release
  - Uploads the compiled examples as a build artifact
- **Example build script** (`scripts/build-examples.ps1`)
  - Compiles all 13 examples into `example-bin/`, copying `duckdb.dll` and `sample_data/` so the binaries run as-is
  - Writes the full compiler output to `example-bin/build.log`
- **README project status section** replacing the "Work in Progress" notice, plus a CI badge

### Changed
- **Documentation accuracy**
  - Version badges and package metadata updated to `1.5.5.1`
  - Lazarus requirement clarified: 4.0+ supported, developed and tested with 4.8
  - Fixed the stale file layout in `CONTRIBUTING.md` (real test files, examples, docs, and scripts) and documented the example build script
  - `docs/TESTING.md` now documents CI and uses a full rebuild command

### Fixed
- `tests/DuckDB.FP.Tests.lpi` now builds from a clean checkout: the default build mode was missing the `..\src` unit path and `..\dll` library path (previously the test project only built when stale compiler units were present)

### Verified
- Tests: 59 tests run, 13 expected error tests, 0 failures
- All 13 examples build and run from `example-bin/`

## [1.5.5] - 2026-09-21

### 🚀 Major Upgrade: DuckDB API 1.5.5 Compatibility

The bundled DuckDB library, C header, and Pascal bindings have been upgraded from 1.3.2 to 1.5.5.

### Added
- **Complete C API bindings**
  - Regenerated `src/libduckdb.pas` from the DuckDB 1.5.5 C header; all 546 functions and 108 types are now available
  - New COPY function API (`duckdb_copy_function_*`), virtual file system API (`duckdb_file_system_*`, `duckdb_file_handle_*`), catalog API (`duckdb_catalog_*`), logging API (`duckdb_log_storage_*`), and custom configuration options (`duckdb_config_option_*`)
  - New cast function API (`duckdb_cast_function_*`), selection vectors (`duckdb_selection_vector`), table descriptions (`duckdb_table_description_*`), and scalar function init/state helpers
  - New types: `DUCKDB_TYPE_GEOMETRY` (40), `DUCKDB_TYPE_VARIANT` (41), plus file flag, config option scope, and catalog entry type enums
- **Version smoke test**
  - Added `TestDuckDBLibraryVersion` to verify the loaded DuckDB library reports v1.5.x

### Changed
- **Renamed API** (`varint` to `bignum`)
  - `duckdb_varint` → `duckdb_bignum`
  - `DUCKDB_TYPE_VARINT` → `DUCKDB_TYPE_BIGNUM`
  - `duckdb_create_varint` → `duckdb_create_bignum`
  - `duckdb_get_varint` → `duckdb_get_bignum`
- **Bundled DuckDB binaries** updated to v1.5.5 in `dll/`, `tests/`, and all `examples/` directories
- **C header** renamed to `c_header/duckdb_1.5.5.h`
- **Documentation** updated for DuckDB v1.5.5 prerequisites

### Verified
- Test suite: 59 tests run, 13 expected error tests, 0 failures
- All examples and the Lazarus package compile against the new bindings

## [1.0.1] - 2025-08-08

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
- **Lazarus pagkage**
  - Updated to version 1.0.1
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
