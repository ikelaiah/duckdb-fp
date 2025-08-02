# Contributing to DuckDB for FreePascal

Thank you for your interest in contributing to the DuckDB FreePascal wrapper! This document provides guidelines and information for contributors.

## 🚀 Getting Started

### Prerequisites
- FreePascal 3.2.2 or later
- Lazarus 4.0+ (recommended for development)
- DuckDB DLL v1.3.2 or later
- Git for version control

### Setting Up Development Environment

1. **Fork and Clone**
   ```bash
   git clone https://github.com/ikelaiah/duckdb-fp.git
   cd duckdb-fp
   ```

2. **Install Dependencies**
   - Ensure FreePascal and Lazarus are installed
   - Download DuckDB DLL and place it in your system PATH or project directory

3. **Open in Lazarus**
   - Open the test project in `tests/` directory
   - Build and run tests to ensure everything works

## 📋 How to Contribute

### Reporting Bugs
- Use the [Bug Report template](.github/ISSUE_TEMPLATE/bug_report.yml)
- Include detailed steps to reproduce
- Provide code samples when possible
- Specify your environment (OS, FreePascal version, DuckDB version)

### Suggesting Features
- Use the [Feature Request template](.github/ISSUE_TEMPLATE/feature_request.yml)
- Describe your use case clearly
- Provide examples of how the feature should work

### Documentation Improvements
- Use the [Documentation Issue template](.github/ISSUE_TEMPLATE/documentation.yml)
- Help improve README, API docs, or code examples
- Fix typos, clarify confusing sections

## 🔧 Development Guidelines

### Code Style
- Follow Pascal naming conventions:
  - Classes: `TClassName`
  - Methods: `MethodName`
  - Properties: `PropertyName`
  - Variables: `VariableName`
  - Constants: `CONSTANT_NAME`

- Use meaningful variable and method names
- Add comments for complex logic
- Keep methods focused and concise

### File Organization
```
src/
├── DuckDB.Wrapper.pas      # Core DuckDB connection wrapper
├── DuckDB.DataFrame.pas    # DataFrame implementation
├── DuckDB.SampleData.pas   # Sample datasets
└── libduckdb.pas          # DuckDB C API bindings

tests/
├── TestDuckDBWrapper.pas   # Unit tests for wrapper
├── TestDataFrame.pas       # Unit tests for DataFrame
└── TestRunner.pas         # Test runner

docs/
├── DuckDB.Wrapper.md      # API documentation
├── DuckDB.DataFrame.md    # DataFrame API docs
└── DuckDB.SampleData.md   # Sample data docs

examples/
├── BasicUsage/            # Basic usage examples
├── DataAnalysis/          # Data analysis examples
└── FileHandling/          # File I/O examples
```

### Testing
- Write unit tests for new features
- Ensure all existing tests pass
- Test on multiple platforms if possible
- Include edge cases and error conditions

### Compatibility
- Maintain compatibility with FreePascal 3.2.2+
- Support DuckDB 1.3.2+
- Consider cross-platform compatibility (Windows, Linux, macOS)

## 📝 Pull Request Process

1. **Create a Branch**
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/your-bug-fix
   ```

2. **Make Changes**
   - Follow coding standards
   - Add tests for new functionality
   - Update documentation as needed

3. **Test Your Changes**
   - Run all unit tests
   - Test manually with different scenarios
   - Verify no regressions

4. **Commit Changes**
   ```bash
   git add .
   git commit -m "feat: add new DataFrame operation"
   # or
   git commit -m "fix: resolve memory leak in query execution"
   ```

5. **Push and Create PR**
   ```bash
   git push origin feature/your-feature-name
   ```
   - Create a pull request using the provided template
   - Fill out all relevant sections
   - Link related issues

## 🏷️ Commit Message Convention

Use conventional commits format:
- `feat:` new features
- `fix:` bug fixes
- `docs:` documentation changes
- `test:` adding or updating tests
- `refactor:` code refactoring
- `perf:` performance improvements
- `chore:` maintenance tasks

Examples:
```
feat: add correlation analysis methods to DataFrame
fix: resolve access violation in ValueCounts method
docs: update API reference for new join operations
test: add unit tests for CSV import functionality
```

## 🧪 Testing Guidelines

### Unit Tests
- Located in `tests/` directory
- Use FreePascal's built-in testing framework
- Test both success and failure scenarios
- Mock external dependencies when possible

### Integration Tests
- Test real DuckDB operations
- Use sample data for consistent results
- Test file I/O operations with temporary files

### Manual Testing
- Test with different data sizes
- Verify memory usage and performance
- Test on different operating systems

## 📚 Documentation

### Code Documentation
- Document all public methods and properties
- Use Pascal XML documentation format
- Include usage examples in comments

### API Documentation
- Update relevant `.md` files in `docs/`
- Include code examples
- Document parameters and return values

### README Updates
- Update feature lists
- Add new examples if applicable
- Update version compatibility information

## 🤝 Community Guidelines

### Be Respectful
- Use inclusive language
- Be patient with newcomers
- Provide constructive feedback

### Communication
- Use GitHub issues for bug reports and feature requests
- Join discussions in pull requests
- Be clear and concise in communications

### Help Others
- Answer questions from other contributors
- Review pull requests when possible
- Share knowledge and best practices

## 🎯 Priority Areas

We especially welcome contributions in these areas:

1. **Performance Optimization**
   - Memory usage improvements
   - Query execution speed
   - Large dataset handling

2. **New DataFrame Operations**
   - Statistical functions
   - Data transformation methods
   - Aggregation operations

3. **File Format Support**
   - Additional CSV options
   - JSON support
   - Excel file support

4. **Documentation and Examples**
   - More real-world examples
   - Tutorial content
   - API documentation improvements

5. **Testing and Quality**
   - Increase test coverage
   - Cross-platform testing
   - Performance benchmarks

## 📞 Getting Help

- **Issues**: Create a GitHub issue for bugs or questions
- **Discussions**: Use GitHub Discussions for general questions
- **Email**: Contact maintainers for sensitive issues

## 📜 License

By contributing to this project, you agree that your contributions will be licensed under the MIT License.

---

Thank you for contributing to DuckDB for FreePascal! 🦆
