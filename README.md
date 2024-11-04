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

## Quick Reference - Open, Close, Query

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

## Quick Reference - Query and Analyze Results

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
    DB.Open();
    
    // Execute query and analyze results
    DF := DB.Query('SELECT * FROM my_table');
    try
      // Display comprehensive statistics
      DF.Describe;
      
      // Check for missing data
      var NullCounts := DF.NullCount;
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

## DataFrame Features

The `TDuckFrame` class provides:
- Column-oriented data storage
- Easy access to data by column name or index
- Pretty printing of results
- Head/Tail operations
- CSV export capabilities
- Comprehensive statistical analysis:
  - Basic statistics (mean, standard deviation)
  - Distribution analysis (quartiles, skewness, kurtosis)
  - Data quality metrics (null counts, non-missing rates)
  - Memory usage reporting

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

## API Reference

- [DuckDB.Wrapper API Reference](DuckDB.Wrapper-API-Ref.md)
- [DuckDB.DataFrame API Reference](DuckDB.DataFrame-API-Ref.md)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## Acknowledgments

- [DuckDB Team](https://duckdb.org/) for the amazing database engine.
- [Free Pascal Dev Team](https://www.freepascal.org/) for the Pascal compiler.
- [Lazarus IDE Team](https://www.lazarus-ide.org/) for such an amazing IDE.
- [rednose🇳🇱🇪🇺](https://discord.com/channels/570025060312547359/570025355717509147/1299342586464698368) of the Unofficial ree Pascal Discord for providing the initial DuckDB Pascal bindings  via [Chet](https://discord.com/channels/570025060312547359/570025355717509147/1299342586464698368).

