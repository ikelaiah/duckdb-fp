```pascal
program LoadFromDuckDBResult;

{$mode objfpc}{$H+}{$J-}

(*
  
  This example demonstrates how to get a TDuckFrame when using DuckDB C API:

  1. Creating an in-memory DuckDB database
  2. Opening a connection
  3. Creating a table and inserting data
  4. Querying the table and displaying results
  5. Proper cleanup/closing of resources

  Note: 
    - This demo uses an in-memory database using DuckDB C API. 
    - The data will not persist between program runs.
    - For simplicity, consider using 
      - the TDuckFrame.CreateFromDuckDB(const ADatabase, ATableName: string);
        constructor to get a TDuckFrame.
      - Or the TDuckDBConnection class for simple DB operations.
*)

uses
  SysUtils, libduckdb, DuckDB.DataFrame;

// Procedure to check the DuckDB state and handle errors
procedure CheckError(state: duckdb_state; const msg: string);
begin
  if state = DuckDBError then
  begin
    WriteLn('Error: ', msg);
    Halt(1); // Terminate the program if an error occurs
  end;
end;

var
  db: duckdb_database;        // DuckDB database handle
  conn: duckdb_connection;    // DuckDB connection handle
  result: duckdb_result;      // DuckDB query result
  state: duckdb_state;        // DuckDB state to track operations
  Results_DF: TDuckFrame;     // DataFrame to store query results

begin
  try
    // Step 1: Create an in-memory DuckDB database
    state := duckdb_open(nil, @db); // Open a new DuckDB database in memory
    CheckError(state, 'Failed to open database');

    // Step 2: Create a connection to the database
    state := duckdb_connect(db, @conn); // Connect to the in-memory DuckDB database
    CheckError(state, 'Failed to connect to database');

    // Step 3: Create a table and insert data
    WriteLn('Creating table and inserting data...');
    state := duckdb_query(conn,
      'CREATE TABLE IF NOT EXISTS users(' +
      'id INTEGER, ' +
      'name VARCHAR, ' +
      'age INTEGER);' +
      'INSERT INTO users VALUES ' +
      '(1, ''Alice'', 25), ' +
      '(2, ''Bob'', 30), ' +
      '(3, ''Charlie'', 35);',
      @result); // Execute SQL to create table and insert sample data
    CheckError(state, 'Failed to create table and insert data');
    duckdb_destroy_result(@result); // Clean up the result object

    // Step 4: Query the table
    WriteLn('Querying table...');
    state := duckdb_query(conn,
      'SELECT * FROM users ORDER BY id;',
      @result); // Execute SQL to retrieve data from the table
    CheckError(state, 'Failed to query table');

    try
      // Create a new DataFrame to hold the query results
      Results_DF := TDuckFrame.Create;

      // Load data from DuckDB result into the DataFrame
      Results_DF.LoadFromResult(@result);

      // Print the top 5 rows of the DataFrame
      Results_DF.Head
                .Print;
    finally
      // Ensure the DataFrame is freed to release memory
      Results_DF.Free;
    end;

    // Delete all rows from the table before closing
    WriteLn;
    WriteLn('Deleting all rows from the table...');
    duckdb_destroy_result(@result);  // Destroy previous result before new query
    
    state := duckdb_query(conn, 'DELETE FROM users;', @result); // Execute SQL to delete all rows
    CheckError(state, 'Failed to delete rows from table');

  finally
    // Step 5: Clean up resources
    duckdb_destroy_result(@result); // Destroy the final result object
    duckdb_disconnect(@conn);       // Disconnect from the database
    duckdb_close(@db);             // Close the database connection
  end;

  // Pause console to allow user to see output
  WriteLn('Press enter to quit ...');
  ReadLn; // Wait for user input
end.
``` 
