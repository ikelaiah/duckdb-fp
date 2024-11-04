# DuckDB FreePascal Wrapper

A clean and intuitive wrapper for DuckDB in FreePascal, providing easy database operations and DataFrame-like result handling.

## ⚠️ Work in Progress

This project is currently under active development. While the basic functionality is working, you may encounter:
- Incomplete features
- API changes
- Missing documentation
- Potential bugs

Feel free to:
- Report issues
- Suggest improvements
- Contribute code
- Test and provide feedback

Current development focus:
- [ ] Complete DataFrame functionality
- [ ] Add more examples
- [ ] Improve error handling
- [ ] Add comprehensive unit tests
- [ ] Document all features

Last tested with:
- FreePascal 3.2.2
- DuckDB 1.1.2
- Lazarus 3.6

## Overview

This wrapper provides a simple interface to work with DuckDB in FreePascal applications, featuring a DataFrame-like structure for handling query results similar to R or Python pandas.

## Features

- Simple, intuitive API
- Automatic resource management
- DataFrame-like structure for query results
- Error handling
- Support for both in-memory and file-based databases
- Transaction support

## Installation

1. Add the following files to your project:
   - `src/DuckDB.Wrapper.pas`
   - `src/DuckDB.DataFrame.pas`
   - `src/libduckdb.pas`

2. Make sure the DuckDB library (DLL/SO) is in your application's path

3. Add the units to your uses clause:

   ```pascal
   uses
     DuckDB.Wrapper, DuckDB.DataFrame;
   ```

## Quick Reference

```pascal
uses
  DuckDB.Wrapper, DuckDB.DataFrame;

var
  DB: TDuckDBConnection;
  DF: TDuckFrame;
begin
  // Create and open in-memory database
  DB := TDuckDBConnection.Create;
  try
    DB.Open();  // or DB.Open('mydata.db') for file-based

    // Execute SQL that doesn't return results
    DB.ExecuteSQL('CREATE TABLE test (id INT, name TEXT)');

    // Query with DataFrame result
    DF := DB.Query('SELECT * FROM test');
    try
      DF.Print;  // Display results
      WriteLn(DF.ValuesByName[0, 'name']);  // Access data
    finally
      DF.Free;
    end;

    // Get single value
    WriteLn(DB.QueryValue('SELECT COUNT(*) FROM test'));

    // Transaction support
    DB.BeginTransaction;
    try
      // ... do work ...
      DB.Commit;
    except
      DB.Rollback;
      raise;
    end;

  finally
    DB.Free;  // Automatically closes connection
  end;
end;
```

## DataFrame Features

The `TDuckFrame` class provides:
- Column-oriented data storage
- Easy access to data by column name or index
- Pretty printing of results
- Head/Tail operations
- Data filtering
- CSV export capabilities

## Error Handling

The wrapper uses exceptions for error handling:

```pascal
try
  DB := TDuckDBConnection.Create;
  DB.Open();
  // ... use the connection
except
  on E: EDuckDBError do
    WriteLn('DuckDB Error: ', E.Message);
  on E: Exception do
    WriteLn('Error: ', E.Message);
end;
```

## Examples

The `examples/` folder contains sample applications demonstrating how to use the wrapper:

### DemoWrapper01
Located in `examples/DemoWrapper01/DemoWrapper01.lpr`, this example shows:
- Creating an in-memory DuckDB database
- Creating tables and inserting data
- Querying data into a DataFrame
- Displaying data with DataFrame.Print
- Exporting data to CSV
- Using Head() to get first N rows
- Accessing data by column name

To run the example:

```pascal
cd examples/DemoWrapper01
lazbuild DemoWrapper01.lpr
./DemoWrapper01
```

The example will:
1. Create a sample table
2. Insert test data
3. Display the data in various ways
4. Save the data to 'test_output.csv'
5. Show how to access specific columns

More examples will be added to demonstrate other features like:
- File-based databases
- Transactions
- Data filtering
- Complex queries

### Demo01
Located in `examples/Demo01/Demo01.lpr`, this example shows:

- Printing the DuckDB version
- Opening an in-memory database
- Creating a connection to the database
- Executing a simple query
- Displaying the results
  - Printing the number of columns and rows
  - Printing column names
  - Printing data rows


... without using the wrapper and dataframe classes.

To run the example:

```pascal
cd examples/Demo01
lazbuild Demo01.lpr
./Demo01
```

### Demo02
Located in `examples/Demo02/Demo02.lpr`, this example shows:
- Printing the DuckDB version
- Opening a file-based database
- Creating a connection to the database
- Executing a simple query
- Displaying the results
  - Printing the number of columns and rows
  - Printing column names
  - Printing data rows

... without using the wrapper and dataframe classes.

To run the example:

```pascal
cd examples/Demo02
lazbuild Demo02.lpr
./Demo02
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## Acknowledgments

- [DuckDB Team](https://duckdb.org/) for the amazing database engine.
- [Free Pascal Dev Team](https://www.freepascal.org/) for the Pascal compiler.
- [Lazarus IDE Team](https://www.lazarus-ide.org/) for such an amazing IDE.
- [rednose🇳🇱🇪🇺](https://discord.com/channels/570025060312547359/570025355717509147/1299342586464698368) of the Unofficial ree Pascal Discord for providing the initial DuckDB Pascal bindings  via [Chet](https://discord.com/channels/570025060312547359/570025355717509147/1299342586464698368).

## DuckDB.Wrapper API Reference

### TDuckDBConnection

Main class for managing DuckDB database connections and operations.

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `IsConnected` | `Boolean` | Returns true if connected to a database |
| `DatabasePath` | `string` | Returns the path of the current database file (empty for in-memory) |

#### Constructor Methods

```pascal
constructor Create; overload;
constructor Create(const ADatabasePath: string); overload;
```
- Creates a new DuckDB connection
- The parameterless version creates without connecting
- The parameterized version automatically connects to the specified database

#### Connection Management

```pascal
procedure Open(const ADatabasePath: string = '');
```
- Opens a connection to a DuckDB database
- Empty path creates an in-memory database
- Specified path opens/creates a file-based database

```pascal
procedure Close;
```
- Closes the current connection
- Automatically called by destructor

```pascal
function Clone: TDuckDBConnection;
```
- Creates a new connection to the same database
- Useful for concurrent operations

#### Query Execution

```pascal
procedure ExecuteSQL(const ASQL: string);
```
- Executes SQL that doesn't return results (CREATE, INSERT, UPDATE, etc.)
- Raises `EDuckDBError` on failure

```pascal
function Query(const ASQL: string): TDuckFrame;
```
- Executes SQL that returns results (SELECT)
- Returns results as a DataFrame
- Caller must free the returned TDuckFrame
- Raises `EDuckDBError` on failure

```pascal
function QueryValue(const ASQL: string): Variant;
```
- Executes SQL that returns a single value
- Returns Null if query returns no results
- Useful for COUNT, SUM, etc.
- Raises `EDuckDBError` on failure

#### Transaction Management

```pascal
procedure BeginTransaction;
```
- Starts a new transaction
- Raises `EDuckDBError` if already in transaction

```pascal
procedure Commit;
```
- Commits the current transaction
- Raises `EDuckDBError` if no active transaction

```pascal
procedure Rollback;
```
- Rolls back the current transaction
- Raises `EDuckDBError` if no active transaction

#### Error Handling

All methods can raise `EDuckDBError` which includes:
- Detailed error message
- SQL state (when available)
- Original DuckDB error code

### Usage Examples

#### In-Memory Database
```pascal
var
  DB: TDuckDBConnection;
begin
  DB := TDuckDBConnection.Create;
  try
    DB.Open();  // In-memory database
    DB.ExecuteSQL('CREATE TABLE test (id INT)');
  finally
    DB.Free;
  end;
end;
```

#### File-Based Database
```pascal
var
  DB: TDuckDBConnection;
begin
  DB := TDuckDBConnection.Create('mydata.db');  // or
  DB := TDuckDBConnection.Create;
  try
    DB.Open('mydata.db');
    // Use database
  finally
    DB.Free;
  end;
end;
```

#### Transaction Example
```pascal
var
  DB: TDuckDBConnection;
begin
  DB := TDuckDBConnection.Create;
  try
    DB.Open();
    DB.BeginTransaction;
    try
      DB.ExecuteSQL('INSERT INTO test VALUES (1)');
      DB.ExecuteSQL('UPDATE other SET value = 42');
      DB.Commit;
    except
      DB.Rollback;
      raise;
    end;
  finally
    DB.Free;
  end;
end;
```

#### Query with DataFrame
```pascal
var
  DB: TDuckDBConnection;
  DF: TDuckFrame;
begin
  DB := TDuckDBConnection.Create;
  try
    DB.Open();
    DF := DB.Query('SELECT * FROM test');
    try
      DF.Print;  // Display results
    finally
      DF.Free;
    end;
  finally
    DB.Free;
  end;
end;
```

#### Single Value Query
```pascal
var
  DB: TDuckDBConnection;
  Count: Variant;
begin
  DB := TDuckDBConnection.Create;
  try
    DB.Open();
    Count := DB.QueryValue('SELECT COUNT(*) FROM test');
    WriteLn('Count: ', Count);
  finally
    DB.Free;
  end;
end;
```

## DuckDB.DataFrame API Reference

### TDuckFrame

A column-oriented data structure for handling DuckDB query results, similar to pandas DataFrame in Python.

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `ColumnCount` | `Integer` | Number of columns in the DataFrame |
| `RowCount` | `Integer` | Number of rows in the DataFrame |
| `Columns[Index: Integer]` | `TDuckDBColumn` | Access column by index |
| `ColumnsByName[Name: string]` | `TDuckDBColumn` | Access column by name |
| `Values[Row, Col: Integer]` | `Variant` | Access value by row and column index |
| `ValuesByName[Row: Integer; ColName: string]` | `Variant` | Access value by row and column name |

#### Column Information (TDuckDBColumn)

| Property | Type | Description |
|----------|------|-------------|
| `Name` | `string` | Column name |
| `DataType` | `TDuckDBColumnType` | Column data type |
| `Data` | `array of Variant` | Column values |

#### Data Types

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

#### Constructor Methods

```pascal
constructor Create;
```
- Creates an empty DataFrame

#### Data Access Methods

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

#### Data Manipulation Methods

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

#### Output Methods

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

### Usage Examples

#### Basic Data Access
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

#### Data Filtering

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

#### Column Selection
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

#### CSV Export
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
