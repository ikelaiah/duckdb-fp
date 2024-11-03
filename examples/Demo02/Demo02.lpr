program Demo02;

{$mode objfpc}{$H+}

(*
  DuckDB Basic Demo
  This example demonstrates:
  1. Creating a persistent DuckDB database file
  2. Opening a connection
  3. Creating a table and inserting data
  4. Querying the table and displaying results
  5. Proper cleanup/closing of resources

  Note: This demo creates/uses a database file named 'mydata.db'.
  The data will persist between program runs.
*)

uses
  SysUtils, libduckdb;

procedure CheckError(state: duckdb_state; const msg: string);
begin
  if state = DuckDBError then
  begin
    WriteLn('Error: ', msg);
    Halt(1);
  end;
end;

var
  db: duckdb_database;
  conn: duckdb_connection;
  result: duckdb_result;
  state: duckdb_state;
  i, j: idx_t;
  value: PUTF8Char;
  dbPath: string;
begin
  // Print DuckDB version
  WriteLn('DuckDB Version: ', duckdb_library_version());
  WriteLn;

  try
    // Step 1: Open/create a persistent database file
    dbPath := 'mydata.db';
    WriteLn('Opening database: ', dbPath);
    state := duckdb_open(PChar(dbPath), @db);
    CheckError(state, 'Failed to open database');

    // Step 2: Create a connection to the database
    state := duckdb_connect(db, @conn);
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
      @result);
    CheckError(state, 'Failed to create table and insert data');
    duckdb_destroy_result(@result);

    // Step 4: Query the table
    WriteLn('Querying table...');
    state := duckdb_query(conn,
      'SELECT * FROM users ORDER BY id;',
      @result);
    CheckError(state, 'Failed to query table');

    // Display the results
    WriteLn('Columns: ', duckdb_column_count(@result));
    WriteLn('Rows: ', duckdb_row_count(@result));

    // Print column names
    for i := 0 to duckdb_column_count(@result) - 1 do
    begin
      Write(duckdb_column_name(@result, i));
      if i < duckdb_column_count(@result) - 1 then
        Write(', ')
      else
        WriteLn;
    end;

    // Print data rows
    for i := 0 to duckdb_row_count(@result) - 1 do
    begin
      for j := 0 to duckdb_column_count(@result) - 1 do
      begin
        value := duckdb_value_varchar(@result, j, i);
        if value <> nil then
        begin
          Write(value);
          duckdb_free(value);
        end
        else
          Write('NULL');

        if j < duckdb_column_count(@result) - 1 then
          Write(', ')
        else
          WriteLn;
      end;
    end;

    // Delete all rows from the table before closing
    WriteLn;
    WriteLn('Deleting all rows from the table...');
    duckdb_destroy_result(@result);  // Destroy previous result before new query
    
    state := duckdb_query(conn, 'DELETE FROM users;', @result);
    CheckError(state, 'Failed to delete rows from table');

  finally
    // Step 5: Clean up resources
    duckdb_destroy_result(@result);
    duckdb_disconnect(@conn);
    duckdb_close(@db);
  end;
end.
