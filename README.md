# DuckDB for FreePascal: An Intuitive Database Wrapper

A simple interface to work with DuckDB in FreePascal applications, featuring a DataFrame-like structure for handling query results similar to R or Python pandas.


## Table of Contents

- [DuckDB for FreePascal: An Intuitive Database Wrapper](#duckdb-for-freepascal-an-intuitive-database-wrapper)
  - [Table of Contents](#table-of-contents)
  - [⚠️ Work in Progress](#️-work-in-progress)
  - [Getting Started with DuckDB for FreePascal](#getting-started-with-duckdb-for-freepascal)
    - [Prerequisites](#prerequisites)
    - [Installation](#installation)
    - [Getting Started from Scratch](#getting-started-from-scratch)
    - [Getting Started with DuckDB Tables](#getting-started-with-duckdb-tables)
    - [Getting Started with CSV Files](#getting-started-with-csv-files)
    - [Working with Parquet Files](#working-with-parquet-files)
    - [Common Operations](#common-operations)
      - [Analyzing Data](#analyzing-data)
      - [Combining DataFrames](#combining-dataframes)
    - [Error Handling](#error-handling)
    - [Next Steps](#next-steps)
  - [API Reference](#api-reference)
  - [Features](#features)
  - [Contributing](#contributing)
  - [License](#license)
  - [Acknowledgments](#acknowledgments)


## ⚠️ Work in Progress

This project is currently under active development. Do expect bugs, missing features and API changes.

Current development focus:
- [ ] Better DataFrame functionality
- [ ] More examples

Last tested with:
- FreePascal 3.2.2
- DuckDB 1.1.2
- Lazarus 3.6
- Win 11

## Getting Started with DuckDB for FreePascal

This guide will help you get started with the DuckDB FreePascal wrapper, covering the most common use cases.

### Prerequisites

- FreePascal 3.2.2 or later
- Lazarus 3.6 (to run examples and tests)
- DuckDB DLL v1.1.2 or later

### Installation

1. Add these files to your project:
   - `src/DuckDB.Wrapper.pas`
   - `src/DuckDB.DataFrame.pas`
   - `src/DuckDB.SampleData.pas` (optional, for sample datasets)
   - `src/libduckdb.pas`

2. Ensure the DuckDB library (DLL/SO) is in your application's path

3. Add the required units to your project:
```pascal
uses
  DuckDB.Wrapper, DuckDB.DataFrame;
```

### Getting Started from Scratch

Create a new DataFrame with custom columns and add data:

```pascal
var
  DuckFrame: TDuckFrame;
begin
  // Create DataFrame with specified columns and types
  DuckFrame := TDuckFrame.CreateBlank(['Name', 'Age', 'City'],
                                    [dctString, dctInteger, dctString]);
  try
    // Add rows
    DuckFrame.AddRow(['John', 25, 'New York']);
    DuckFrame.AddRow(['Alice', 30, 'Boston']);
    
    // Display the DataFrame
    DuckFrame.Print;
    
    // Save to CSV if needed
    DuckFrame.SaveToCSV('output.csv');
  finally
    DuckFrame.Free;
  end;
end;
```

### Getting Started with DuckDB Tables

Connect to a DuckDB database and query existing tables:

```pascal
var
  DB: TDuckDBConnection;
  Frame: TDuckFrame;
begin
  DB := TDuckDBConnection.Create;
  try
    // Connect to database (use ':memory:' for in-memory database)
    DB.Open('mydata.db');
    
    // Query existing table
    Frame := DB.Query('SELECT * FROM my_table');
    try
      // Basic operations
      Frame.Print;                    // Display data
      Frame.Describe;                 // Show statistical summary
      Frame.Info;                     // Show structure info
      
      // Basic analysis
      WriteLn('Row count: ', Frame.RowCount);
      
      // Access specific values
      WriteLn(Frame.ValuesByName[0, 'column_name']);
      
    finally
      Frame.Free;
    end;
  finally
    DB.Free;
  end;
end;
```

### Getting Started with CSV Files

Load data from CSV files and analyze it:

```pascal
var
  DB: TDuckDBConnection;
  Frame: TDuckFrame;
begin
  DB := TDuckDBConnection.Create;
  try
    DB.Open();
    
    // Read CSV file into DataFrame
    Frame := DB.ReadCSV('data.csv');
    try
      // Basic exploration
      Frame.Head(5);                  // Show first 5 rows
      
      // Check for missing data
      Frame.NullCount.Print;
      
      // Get basic statistics
      Frame.Describe;
      
      // Save processed data
      Frame.SaveToCSV('processed.csv');
      
    finally
      Frame.Free;
    end;
  finally
    DB.Free;
  end;
end;
```

### Working with Parquet Files

Create a DataFrame from Parquet files:

```pascal
var
  Frame: TDuckFrame;
begin
  // Load single Parquet file
  Frame := TDuckFrame.CreateFromParquet('data.parquet');
  try
    Frame.Print;  // Display the data
  finally
    Frame.Free;
  end;
end;
```

Load multiple Parquet files as a single DataFrame:

```pascal
var
  Frame: TDuckFrame;
  Files: array of string;
begin
  SetLength(Files, 3);
  Files[0] := 'data1.parquet';
  Files[1] := 'data2.parquet';
  Files[2] := 'data3.parquet';
  
  // Load multiple files
  Frame := TDuckFrame.CreateFromParquet(Files);
  try
    WriteLn('Total rows: ', Frame.RowCount);
    Frame.Print;
  finally
    Frame.Free;
  end;
end;
```


### Common Operations

#### Analyzing Data

```pascal
// Statistical summary
Frame.Describe;

// Structure information
Frame.Info;

// Missing value analysis
Frame.NullCount.Print;

// First/last rows
Frame.Head(5);  // First 5 rows
Frame.Tail(5);  // Last 5 rows

// Correlation analysis
Frame.CorrPearson.Print;  // Pearson correlation
Frame.CorrSpearman.Print; // Spearman correlation
```

#### Combining DataFrames

```pascal
var
  Combined: TDuckFrame;
begin
  // Union with duplicate removal
  Combined := Frame1.Union(Frame2);
  
  // Union keeping all rows
  Combined := Frame1.UnionAll(Frame2);
  
  // Union modes:
  // umStrict - Requires exact column match
  // umCommon - Only common columns
  // umAll    - All columns (NULL for missing)
  Combined := Frame1.Union(Frame2, umCommon);
end;
```

### Error Handling

Always use try-finally blocks and handle exceptions:

```pascal
try
  // Your DuckDB operations here
except
  on E: EDuckDBError do
    WriteLn('DuckDB Error: ', E.Message);
  on E: Exception do
    WriteLn('Error: ', E.Message);
end;
```

### Next Steps

- Check the [examples folder](examples/) for more detailed examples
- Read the API documentation for other features
- Check the [TESTING.md](TESTING.md) file for information on how to run and maintain the test suite

## API Reference

- [DuckDB.Wrapper API Reference](docs/DuckDB.Wrapper.md)
- [DuckDB.DataFrame API Reference](docs/DuckDB.DataFrame.md)
- [DuckDB.SampleData API Reference](docs/DuckDB.SampleData.md)

## Features

- Native DuckDB integration
- DataFrame operations similar to pandas/R
- CSV file handling:
  - Read CSV files with automatic type inference (`TDuckDBConnection.ReadCSV`)
  - Save DataFrames to CSV with RFC 4180 compliance (`TDuckFrame.SaveToCSV`)
- Data analysis capabilities:
  - Basic statistics (mean, std dev, etc.)
  - Correlation analysis (Pearson and Spearman)
  - Frequency counts
  - Missing value handling
- Pretty printing with customizable row limits
- Column selection and filtering
- Descriptive statistics
- DataFrame Operations:
  - Data Analysis: 
    - `Describe`: Comprehensive statistical summary
    - `Info`: DataFrame structure information
    - `NullCount`: Count of null values per column
    - `Head`, `Tail`: View first/last N rows
  - Statistical Analysis: 
    - `CorrPearson`: Pearson correlation matrix
    - `CorrSpearman`: Spearman correlation matrix
  - Data Export: `SaveToCSV`
  - DataFrame Combinations:
    - `Union`: Combines DataFrames and removes duplicates (like SQL UNION)
    - `UnionAll`: Combines DataFrames keeping all rows (like SQL UNION ALL)
    - `Distinct`: Removes duplicate rows from DataFrame
    - Flexible union modes:
      - `umStrict`: Requires exact match of column names and types
      - `umCommon`: Only includes columns that exist in both frames
      - `umAll`: Includes all columns, filling missing values with NULL


## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## Acknowledgments

- [DuckDB Team](https://duckdb.org/) for the amazing database engine.
- [Free Pascal Dev Team](https://www.freepascal.org/) for the Pascal compiler.
- [Lazarus IDE Team](https://www.lazarus-ide.org/) for such an amazing IDE.
- [rednose🇳🇱🇪🇺](https://discord.com/channels/570025060312547359/570025355717509147/1299342586464698368) of the [Unofficial Free Pascal discord server](https://discord.com/channels/570025060312547359/570091337173696513) for providing the initial DuckDB Pascal bindings via [Chet](https://discord.com/channels/570025060312547359/570025355717509147/1299342586464698368).
- The kind and helpful individuals on various online platforms such as;
    - [Unofficial Free Pascal discord server](https://discord.com/channels/570025060312547359/570091337173696513).
    - [Free Pascal & Lazarus forum](https://forum.lazarus.freepascal.org/index.php).
    - [Tweaking4All Delphi, Lazarus, Free Pascal forum](https://www.tweaking4all.com/forum/delphi-lazarus-free-pascal/).
    - [Laz Planet - Blogspot](https://lazplanet.blogspot.com/) / [Laz Planet - GitLab](https://lazplanet.gitlab.io/).
    - [Delphi Basics](https://www.delphibasics.co.uk/index.html).