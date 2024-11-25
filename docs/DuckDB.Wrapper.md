# DuckDB.Wrapper API Reference

## Table of Contents

- [DuckDB.Wrapper API Reference](#duckdbwrapper-api-reference)
  - [Table of Contents](#table-of-contents)
  - [TDuckDBConnection](#tduckdbconnection)
    - [Properties](#properties)
    - [Constructor Methods](#constructor-methods)
    - [Connection Management](#connection-management)
    - [Query Execution](#query-execution)
    - [Transaction Management](#transaction-management)
    - [CSV Operations](#csv-operations)
    - [Data Import/Export](#data-importexport)
    - [Error Handling](#error-handling)
  - [Usage Examples](#usage-examples)
    - [Reading CSV Files](#reading-csv-files)
    - [In-Memory Database](#in-memory-database)
    - [File-Based Database](#file-based-database)
    - [Transaction Example](#transaction-example)
    - [Query with DataFrame](#query-with-dataframe)
    - [Single Value Query](#single-value-query)

## TDuckDBConnection

Main class for managing DuckDB database connections and operations.

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `IsConnected` | `Boolean` | Returns true if connected to a database |
| `DatabasePath` | `string` | Returns the path of the current database file (empty for in-memory) |

### Constructor Methods

```pascal
constructor Create; overload;
constructor Create(const ADatabasePath: string); overload;
```
- Creates a new DuckDB connection
- The parameterless version creates without connecting
- The parameterized version automatically connects to the specified database

### Connection Management

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

### Query Execution

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

### Transaction Management

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

### CSV Operations

```pascal
class function ReadCSV(const FileName: string): TDuckFrame;
```
- Reads a CSV file and returns it as a DataFrame
- Uses DuckDB's automatic type inference
- Handles headers automatically
- Raises `EDuckDBError` if file not found or on parsing error
- Caller must free the returned TDuckFrame
- Example:
  ```pascal
  var
    DF: TDuckFrame;
  begin
    DF := TDuckDBConnection.ReadCSV('data.csv');
    try
      DF.Print;  // Display results
    finally
      DF.Free;
    end;
  end;
  ```

### Data Import/Export

```pascal
class function ReadCSV(const FileName: string): TDuckFrame;
```
- Reads a CSV file into a DataFrame using DuckDB's automatic type inference
- Returns a new DataFrame (caller must free)
- Raises `EDuckDBError` if file not found

```pascal
procedure WriteToTable(const DataFrame: TDuckFrame; const TableName: string; 
  const SchemaName: string = 'main');
```
- Writes a DataFrame to a table in the currently connected database
- Uses the existing connection (in-memory if `DatabasePath` is empty, file-based otherwise)
- Creates the table if it doesn't exist
- Uses transactions for data integrity
- Parameters:
  - `DataFrame`: Source DataFrame to write
  - `TableName`: Name of the target table
  - `SchemaName`: Optional schema name (defaults to 'main')
- Raises `EDuckDBError` if:
  - Not connected to database
  - DataFrame is empty
  - SQL errors occur during write

Example usage:
```pascal
var
  DB: TDuckDBConnection;
  DF: TDuckFrame;
begin
  // For in-memory database:
  DB := TDuckDBConnection.Create;
  try
    DB.Open;  // Creates in-memory database
    // Read CSV into DataFrame
    DF := TDuckDBConnection.ReadCSV('data.csv');
    try
      DB.WriteToTable(DF, 'mytable');
    finally
      DF.Free;
    end;
  finally
    DB.Free;
  end;
  
  // For file-based database:
  DB := TDuckDBConnection.Create('mydb.db');  // or DB.Open('mydb.db');
  try
    // Read CSV into DataFrame
    DF := TDuckDBConnection.ReadCSV('data.csv');
    try
      DB.WriteToTable(DF, 'mytable');
      // Or with custom schema:
      // DB.WriteToTable(DF, 'mytable', 'custom_schema');
    finally
      DF.Free;
    end;
  finally
    DB.Free;
  end;
end;
```

### Error Handling

All methods can raise `EDuckDBError` which includes:
- Detailed error message
- SQL state (when available)
- Original DuckDB error code

## Usage Examples

### Reading CSV Files
```pascal
var
  DF: TDuckFrame;
begin
  DF := TDuckDBConnection.ReadCSV('data.csv');
  try
    DF.Print;  // Display contents
    DF.SaveToCSV('output.csv');  // Save to new file
  finally
    DF.Free;
  end;
end;
```

### In-Memory Database
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

### File-Based Database
```pascal
var
  DB: TDuckDBConnection;
begin
  DB := TDuckDBConnection.Create('mydata.db');  // or  DB := TDuckDBConnection.Create;
  try
    DB.Open('mydata.db');
    // Use database
  finally
    DB.Free;
  end;
end;
```

### Transaction Example
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

### Query with DataFrame
```pascal
var
  DB: TDuckDBConnection;
  DF: TDuckFrame;
begin
  DB := TDuckDBConnection.Create;
  try
    DB.Open();  // Creates in-memory database
    
    // Create and populate a test table
    DB.ExecuteSQL('CREATE TABLE test (id INTEGER, name VARCHAR)');
    DB.ExecuteSQL('INSERT INTO test VALUES (1, ''Alice''), (2, ''Bob''), (3, ''Charlie'')');
    
    // Query the table into a DataFrame
    DF := DB.Query('SELECT * FROM test ORDER BY id');
    try
      DF.Print;  // Will display:
      // id   name
      // ---- -------
      // 1    Alice
      // 2    Bob
      // 3    Charlie
    finally
      DF.Free;
    end;
  finally
    DB.Free;
  end;
end;
```

### Single Value Query
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