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
    - [CSV Export](#csv-export)
  - [Data Analysis Methods](#data-analysis-methods)
    - [Describe](#describe)
    - [NullCount](#nullcount)
    - [Info](#info) 

## TDuckFrame

A column-oriented data structure for handling DuckDB query results, similar to pandas DataFrame in Python.

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

```pascal
procedure SaveToCSV(const FileName: string);
```
- Saves the DataFrame to a CSV file
- Includes header row
- Uses standard CSV formatting

### Data Analysis Methods

```pascal
procedure Describe;
```
- Displays comprehensive summary statistics for numeric columns including:
  - count: Total number of rows
  - mean: Average value
  - std: Standard deviation
  - skew: Skewness (measure of distribution asymmetry)
  - kurt: Kurtosis (measure of distribution "tailedness")
  - non-miss: Percentage of non-missing values
  - min: Minimum value
  - 25%: First quartile
  - 50%: Median
  - 75%: Third quartile
  - max: Maximum value
  - null: Number of null values

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
         id           age          salary
count    4            4            4
mean     2.50         33.33        68333.33
std      1.29         10.41        16072.75
skew     0.00         0.29         -0.34
kurt     -2.08        0.00         0.00
non-miss 100.00%      75.00%       75.00%
min      1            25           50000
25%      1.75         27.50        62500.00
50%      2.50         30.00        75000.00
75%      3.25         37.50        77500.00
max      4            45           80000
null     0            1            1

Null Value Counts:
----------------
id  name  age  salary
-- ---- --- ------
0   0     1    1

DataFrame Info:
--------------
DataFrame: 4 rows × 4 columns

Columns:
  id: dctInteger (nulls: 0)
  name: dctString (nulls: 0)
  age: dctInteger (nulls: 1)
  salary: dctDouble (nulls: 1)

Memory usage: 384 bytes (0.00 MB)
Press Enter to exit...
```
