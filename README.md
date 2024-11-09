# DuckDB for FreePascal: An Intuitive Database Wrapper

A simple interface to work with DuckDB in FreePascal applications, featuring a DataFrame-like structure for handling query results similar to R or Python pandas.

## Table of Contents

- [DuckDB for FreePascal: An Intuitive Database Wrapper](#duckdb-for-freepascal-an-intuitive-database-wrapper)
  - [Table of Contents](#table-of-contents)
  - [⚠️ Work in Progress](#️-work-in-progress)
  - [Features](#features)
  - [Installation](#installation)
  - [Quick Reference](#quick-reference)
    - [Open, Query and Close Connection](#open-query-and-close-connection)
    - [Query and Analyze Results](#query-and-analyze-results)
    - [Combining DataFrames](#combining-dataframes)
  - [Error Handling](#error-handling)
  - [Examples](#examples)
    - [DuckFrameFromBlank](#duckframefromblank)
    - [DuckFrameFromDuckDB](#duckframefromduckdb)
    - [DuckFrameBasicAnalysis](#duckframebasicanalysis)
    - [UnionDataFrames](#uniondataframes)
  - [API Reference](#api-reference)
  - [Contributing](#contributing)
  - [License](#license)
  - [Acknowledgments](#acknowledgments)


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

## Installation

1. Add the following files to your project:
   - `src/DuckDB.Wrapper.pas`
   - `src/DuckDB.DataFrame.pas`
   - `src/DuckDB.SampleData.pas` (optional, for sample datasets)
   - `src/libduckdb.pas`

2. Make sure the DuckDB library (DLL/SO) is in your application's path

3. Add the units to your uses clause:

   ```pascal
   uses
     DuckDB.Wrapper, DuckDB.DataFrame;
   ```

## Quick Reference

### Open, Query and Close Connection

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
    DB.Free;
  end;
end;
```

### Query and Analyze Results

```pascal
uses
  DuckDB.Wrapper, DuckDB.DataFrame;

var
  DB: TDuckDBConnection;
  NullCounts, DF: TDuckFrame;
begin
  // Create and open in-memory database
  DB := TDuckDBConnection.Create;
  try
    DB.Open();
    
    // Execute query and analyze results
    DF := DB.Query('SELECT * FROM my_table');
    try
      // Display comprehensive statistics
      DF.Describe;
      
      // Check for missing data
      NullCounts := DF.NullCount;
      try
        NullCounts.Print;
      finally
        NullCounts.Free;
      end;
      
      // Show DataFrame structure and memory usage
      DF.Info;
    finally
      DF.Free;
    end;
  finally
    DB.Free;
  end;
end;
```

### Combining DataFrames
```pascal
var
  DB: TDuckDBConnection;
  DF1, DF2, Combined: TDuckFrame;
begin
  DB := TDuckDBConnection.Create('example.db');
  try
    // Create two DataFrames from queries
    DF1 := DB.Query('SELECT * FROM sales_2022');
    DF2 := DB.Query('SELECT * FROM sales_2023');
    try
      // Combine with duplicate removal
      Combined := DF1.Union(DF2);
      try
        Combined.SaveToCSV('combined_sales.csv');
      finally
        Combined.Free;
      end;
    finally
      DF1.Free;
      DF2.Free;
    end;
  finally
    DB.Free;
  end;
end;
```

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

### [DuckFrameFromBlank](examples/DuckFrameFromBlank/DuckFrameFromBlank.lpr)

This example demonstrates how to create a DataFrame from scratch. 

Snippet:

```pascal
  ...
  DuckFrame := TDuckFrame.CreateBlank(['Name', 'Age', 'City'],
                                      [dctString, dctInteger, dctString]);
  try
    // Adding two rows to this dataframe
    DuckFrame.AddRow(['John', 25, 'New York']);
    ...

    // Print data frame
    DuckFrame.Print;

  finally
    DuckFrame.Free;
  end;
  ...
```

### [DuckFrameFromDuckDB](examples/DuckFrameFromDuckDB/DuckFrameFromDuckDB.lpr)

This example demonstrates how to create a DataFrame from a DuckDB file.

Snippet:  

```pascal
    
    ...
    DataDir := 'data';
    // Read table `customer` from a DuckDB file
    Frame := TDuckFrame.CreateFromDuckDB(DataDir + DirectorySeparator + 'customers.db', 'customers');
    try
      ...
      Frame.Print;  // Add this to see the actual data
      ...

      // Basic statistics for numeric columns
      ...
      if Frame.FindColumnIndex('age') >= 0 then
      begin
        ...
        Frame.PlotHistogram('age', 5);  // Show age distribution in 5 bins
      end;
      
      ...

      // Show unique counts for a categorical column
      ...
      if Frame.FindColumnIndex('country') >= 0 then
      begin
        Frame.UniqueCounts('country').Print;
      end;

    ...

    finally
      Frame.Free;
    end;
  ...
```

### [DuckFrameBasicAnalysis](examples/DuckFrameBasicAnalysis/DuckFrameBasicAnalysis.lpr)

This example demonstrates basic DataFrame operations, including statistical summary, null count, and basic info of dataframe.

Snippet:

```pascal
  ...
  DF := DB.Query('SELECT * FROM employees');
  try
    ...
    DF.Describe; // Show statistical summary
    ...

    // Get Null counts as a DF
    NullCounts := DF.NullCount;
    try
      NullCounts.Print;
    finally
      NullCounts.Free;
    end;
    ...

    DF.Info; // Show DataFrame structure and memory usage
    ...
  finally
    DF.Free;
  end;
  ...
``` 

Sample output:

```
Statistical Summary:
-------------------
Number of rows: 5
Number of columns: 4

Column type frequency:
  factor    1
  numeric   3

-- Variable type: factor
skim_variable    n_missing  complete_rate ordered   n_unique
name             0          1.000         FALSE     5
    Top counts: Bob: 1, Alice: 1, John: 1

-- Variable type: numeric
skim_variable    n_missing  complete_rate mean      sd        min       q1        median    q3        max       skew      kurt
id               0          1.000         3.000     1.581     1.000     2.000     3.000     4.000     5.000     0.000     13.760
age              1          0.800         33.750    8.539     25.000    28.750    32.500    37.500    45.000    0.847     17.646
salary           1          0.800         66250.000 13768.926 50000.000 57500.000 67500.000 76250.000 80000.000 -0.364    10.051

Null Value Counts:
----------------
id  name  age  salary
-- ---- --- ------
0   0     1    1

DataFrame Info:
--------------
DataFrame: 5 rows × 4 columns

Columns:
  id: dctInteger (nulls: 0)
  name: dctString (nulls: 0)
  age: dctInteger (nulls: 1)
  salary: dctDouble (nulls: 1)

Memory usage: 480 bytes (0.00 MB)
```

### [UnionDataFrames](examples/UnionDataFrames/UnionDataFrames.lpr)

This example demonstrates how to combine DataFrames using different union modes.  

Snippet:

```pascal
  ...
DB := TDuckDBConnection.Create(':memory:');
  try
    // Create DataFrames with different structures
    DF1 := DB.Query('SELECT 1 as id, ''A'' as name, 25 as age');
    DF2 := DB.Query('SELECT 2 as id, ''B'' as name, ''HR'' as department');
    
    try
      ...      
      // UnionAll with umCommon (only shared columns)
      Combined := DF1.UnionAll(DF2, umCommon);
      try
        Combined.Print;
        ...
      finally
        Combined.Free;
      end;
      ...
    finally
      DF1.Free;
      DF2.Free;
    end;
...
``` 

## API Reference

- [DuckDB.Wrapper API Reference](DuckDB.Wrapper-API-Ref.md)
- [DuckDB.DataFrame API Reference](DuckDB.DataFrame-API-Ref.md)
- [DuckDB.SampleData API Reference](DuckDB.SampleData-API-Ref.md)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## Acknowledgments

- [DuckDB Team](https://duckdb.org/) for the amazing database engine.
- [Free Pascal Dev Team](https://www.freepascal.org/) for the Pascal compiler.
- [Lazarus IDE Team](https://www.lazarus-ide.org/) for such an amazing IDE.
- [rednose🇳🇱🇪🇺](https://discord.com/channels/570025060312547359/570025355717509147/1299342586464698368) of the Unofficial ree Pascal Discord for providing the initial DuckDB Pascal bindings  via [Chet](https://discord.com/channels/570025060312547359/570025355717509147/1299342586464698368).