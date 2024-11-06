# DuckDB.DataFrame API Reference

## Table of Contents

- [DuckDB.DataFrame API Reference](#duckdbdataframe-api-reference)
  - [TDuckFrame](#tduckframe)
    - [Properties](#properties)
    - [Column Information (TDuckDBColumn)](#column-information-tduckdbcolumn)
    - [Data Types](#data-types)
    - [Constructor Methods](#constructor-methods)
  - [Data Access](#data-access)
    - [Column Selection](#column-selection)
    - [Data Filtering](#data-filtering)
  - [File Operations](#file-operations)
    - [CSV Export](#csv-export)
  - [Data Analysis Methods](#data-analysis-methods)
    - [Describe](#describe)
    - [NullCount](#nullcount)
    - [Info](#info)
    - [Statistical Analysis](#statistical-analysis)
  - [Correlation Analysis](#correlation-analysis)

## TDuckFrame

A column-oriented data structure for handling DuckDB query results, similar to R's data frame or pandas DataFrame in Python.

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `ColumnCount` | `Integer` | Number of columns in the DataFrame |
| `RowCount` | `Integer` | Number of rows in the DataFrame |
| `Columns[Index: Integer]` | `TDuckDBColumn` | Access column by index |
| `ColumnsByName[Name: string]` | `TDuckDBColumn` | Access column by name |
| `Values[Row, Col: Integer]` | `Variant` | Access value by row and column index |
| `ValuesByName[Row: Integer; ColName: string]` | `Variant` | Access value by row and column name |

### Column Information (TDuckDBColumn)

| Property | Type | Description |
|----------|------|-------------|
| `Name` | `string` | Column name |
| `DataType` | `TDuckDBColumnType` | Column data type |
| `Data` | `array of Variant` | Column values |

### Data Types

```pascal
TDuckDBColumnType = (
  dctUnknown,
  dctBoolean,
  dctTinyInt,
  dctSmallInt,
  dctInteger,
  dctBigInt,
  dctFloat,
  dctDouble,
  dctDate,
  dctTimestamp,
  dctString,
  dctBlob
);
```

### Constructor Methods

```pascal
constructor Create;
```
- Creates an empty DataFrame

### Data Access Methods

```pascal
function FindColumnIndex(const Name: string): Integer;
```
- Returns the index of a column by name
- Returns -1 if column not found

```pascal
function GetColumn(Index: Integer): TDuckDBColumn;
```
- Returns column information by index
- Raises exception if index out of bounds

```pascal
function GetColumnByName(const Name: string): TDuckDBColumn;
```
- Returns column information by name
- Raises exception if column not found

### Data Manipulation Methods

```pascal
procedure Clear;
```
- Removes all data from the DataFrame

```pascal
function Head(Count: Integer = 5): TDuckFrame;
```
- Returns a new DataFrame with the first N rows
- Default count is 5
- Caller must free the returned DataFrame

```pascal
function Tail(Count: Integer = 5): TDuckFrame;
```
- Returns a new DataFrame with the last N rows
- Default count is 5
- Caller must free the returned DataFrame

```pascal
function Select(const Columns: array of string): TDuckFrame;
```
- Returns a new DataFrame with only the specified columns
- Caller must free the returned DataFrame

### Output Methods

```pascal
procedure Print;
```
- Prints the DataFrame to console in a formatted table

### Data Analysis Methods

```pascal
procedure Describe;
```
- Displays comprehensive summary statistics with separate analysis for:
  
  Factor (Categorical) Variables:
  - skim_variable: Column name
  - n_missing: Number of missing values
  - complete_rate: Percentage of non-missing values
  - ordered: Whether the categorical variable is ordered
  - n_unique: Number of unique values
  - top_counts: Most frequent values and their counts

  Numeric Variables:
  - skim_variable: Column name
  - n_missing: Number of missing values
  - complete_rate: Percentage of non-missing values
  - mean: Average value
  - sd: Standard deviation
  - min: Minimum value
  - q1: First quartile (25th percentile)
  - median: Median (50th percentile)
  - q3: Third quartile (75th percentile)
  - max: Maximum value
  - skew: Skewness (measure of distribution asymmetry)
  - kurt: Kurtosis (measure of distribution "tailedness")

Example output:
```
Statistical Summary:
-------------------
Number of rows: 4
Number of columns: 4

Column type frequency:
  factor    1
  numeric   3

-- Variable type: factor
skim_variable    n_missing  complete_rate ordered   n_unique
name             0          1.000         FALSE     4
    Top counts: Bob: 1, Alice: 1, John: 1

-- Variable type: numeric
skim_variable    n_missing  complete_rate mean      sd        min       q1        median    q3        max       skew      kurt
id               0          1.000         2.500     1.291     1.000     1.750     2.500     3.250     4.000     0.000     -2.080
age              1          0.750         33.333    10.408    25.000    27.500    30.000    37.500    45.000     0.996     0.000
salary           1          0.750         68333.333 16072.751 50000.000 62500.000 75000.000 77500.000 80000.000 -1.190     0.000
```

```pascal
function NullCount: TDuckFrame;
```
- Returns a new DataFrame with a count of null values for each column
- Result has a single row with the same column names as the original
- Caller must free the returned DataFrame

```pascal
procedure Info;
```
- Displays basic information about the DataFrame including:
  - Number of rows and columns
  - Column names and their data types
  - Number of null values per column
  - Memory usage in both bytes and MB

### Statistical Analysis

```pascal
function CorrPearson: TDuckFrame;
```
- Calculates the Pearson correlation matrix for all numeric columns
- Returns a new DataFrame containing the correlation coefficients
- Measures linear correlation between variables
- Best for linear relationships between variables
- Sensitive to outliers
- Caller must free the returned DataFrame

```pascal
function CorrSpearman: TDuckFrame;
```
- Calculates the Spearman rank correlation matrix for all numeric columns
- Returns a new DataFrame containing the correlation coefficients
- Measures monotonic relationships (including non-linear)
- More robust to outliers than Pearson correlation
- Better for ordinal data and non-linear relationships
- Caller must free the returned DataFrame

Example usage:
```pascal
var
  PearsonCorr, SpearmanCorr: TDuckFrame;
begin
  // Calculate Pearson correlation
  PearsonCorr := DF.CorrPearson;
  try
    WriteLn('Pearson Correlation:');
    PearsonCorr.Print;
  finally
    PearsonCorr.Free;
  end;
  
  // Calculate Spearman correlation
  SpearmanCorr := DF.CorrSpearman;
  try
    WriteLn('Spearman Correlation:');
    SpearmanCorr.Print;
  finally
    SpearmanCorr.Free;
  end;
end;
```

When to use each correlation method:

Pearson Correlation:
- Variables have linear relationships
- Data is normally distributed
- No significant outliers
- Variables are continuous

Spearman Correlation:
- Non-linear but monotonic relationships
- Ordinal data
- Presence of outliers
- Non-normal distributions
- More robust general-purpose correlation

## File Operations

### CSV Export

```pascal
procedure SaveToCSV(const FileName: string);
```
Saves the DataFrame to a CSV file following RFC 4180 specifications.

Features:
- Full RFC 4180 compliance
- Handles special cases:
  - Multi-line text fields (preserved with proper quoting)
  - Fields containing commas
  - Fields containing quotes (escaped with double quotes)
  - NULL values (written as empty fields)
  - Empty string values
- Uses standard CRLF line endings
- Properly escapes and quotes field values when needed

Example:
```pascal
var
  DF: TDuckFrame;
begin
  // ... populate DataFrame ...
  
  // Save to CSV file
  DF.SaveToCSV('output.csv');
end;
```

Output format example:
```csv
id,name,description
1,"Simple text","Normal field"
2,"Text with
multiple lines","Another field"
3,"Text with ""quotes""","Text with, comma"
4,,"Empty field (NULL)"
```

## Usage Examples

### Basic Data Access
```pascal
var
  DF: TDuckFrame;
begin
  DF := DB.Query('SELECT * FROM test');
  try
    // Access by index
    WriteLn(DF.Values[0, 0]);
    
    // Access by column name
    WriteLn(DF.ValuesByName[0, 'id']);
    
    // Get column info
    WriteLn('Column count: ', DF.ColumnCount);
    WriteLn('First column name: ', DF.Columns[0].Name);
  finally
    DF.Free;
  end;
end;
```

### Data Filtering

Use SQL WHERE clauses for data filtering.

```pascal
var
  DB: TDuckDBConnection;
  DF: TDuckFrame;
begin
  DB := TDuckDBConnection.Create;
  try
    DB.Open();
    // Filter directly in SQL
    DF := DB.Query('SELECT * FROM test WHERE id > 10');
    try
      DF.Print;
    finally
      DF.Free;
    end;
  finally
    DB.Free;
  end;
end;
```

### Column Selection
```pascal
var
  DF, SelectedDF: TDuckFrame;
begin
  DF := DB.Query('SELECT * FROM test');
  try
    // Select specific columns
    SelectedDF := DF.Select(['id', 'name']);
    try
      SelectedDF.Print;
    finally
      SelectedDF.Free;
    end;
  finally
    DF.Free;
  end;
end;
```

### CSV Export
```pascal
var
  DF: TDuckFrame;
begin
  DF := DB.Query('SELECT * FROM test');
  try
    DF.SaveToCSV('output.csv');
  finally
    DF.Free;
  end;
end;
```

### Data Analysis Example
```pascal
var
  DF: TDuckFrame;
begin
  DF := DB.Query('SELECT * FROM employees');
  try
    // Display statistical summary
    WriteLn('Statistical Summary:');
    WriteLn('-------------------');
    DF.Describe;
    WriteLn;
    
    // Show null value counts
    WriteLn('Null Value Counts:');
    WriteLn('----------------');
    var NullCounts := DF.NullCount;
    try
      NullCounts.Print;
    finally
      NullCounts.Free;
    end;
    WriteLn;
    
    // Display DataFrame information
    WriteLn('DataFrame Info:');
    WriteLn('--------------');
    DF.Info;
  finally
    DF.Free;
  end;
end;
```

Example output:

```
Statistical Summary:
-------------------
Number of rows: 4
Number of columns: 4

Column type frequency:
  factor    1
  numeric   3

-- Variable type: factor
skim_variable    n_missing  complete_rate ordered   n_unique
name             0          1.000         FALSE     4
    Top counts: Bob: 1, Alice: 1, John: 1

-- Variable type: numeric
skim_variable    n_missing  complete_rate mean      sd        min       q1        median    q3        max       skew      kurt
id               0          1.000         2.500     1.291     1.000     1.750     2.500     3.250     4.000     0.000     -2.080
age              1          0.750         33.333    10.408    25.000    27.500    30.000    37.500    45.000     0.996     0.000
salary           1          0.750         68333.333 16072.751 50000.000 62500.000 75000.000 77500.000 80000.000 -1.190     0.000
```
