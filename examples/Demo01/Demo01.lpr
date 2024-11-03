program DuckDBDemo;

{$mode objfpc}{$H+}

(*
  DuckDB Basic Demo
  This example demonstrates:
  1. Creating an in-memory DuckDB database
  2. Opening a connection
  3. Running a simple query
  4. Displaying results
  5. Proper cleanup/closing of resources

  Note: This demo uses an in-memory database (temporary).
  For a persistent database, use: duckdb_open('mydata.db', @db)
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
begin
  // Print DuckDB version
  WriteLn('DuckDB Version: ', duckdb_library_version());
  WriteLn;

  try
    // Step 1: Open an in-memory database
    // Note: Using nil creates a temporary in-memory database
    // For persistent storage, use: duckdb_open('mydata.db', @db)
    state := duckdb_open(nil, @db);
    CheckError(state, 'Failed to open database');

    // Step 2: Create a connection to the database
    state := duckdb_connect(db, @conn);
    CheckError(state, 'Failed to connect to database');

    // Step 3: Execute a simple query
    state := duckdb_query(conn, 'SELECT 42 AS number, ''Hello'' AS greeting', @result);
    CheckError(state, 'Failed to execute query');

    // Step 4: Display the results
    WriteLn('No. of columns: ', duckdb_column_count(@result));
    WriteLn('No. of rows   : ', duckdb_row_count(@result));
    WriteLn;

    // Print column names
    for i := 0 to duckdb_column_count(@result) - 1 do
    begin
      Write(duckdb_column_name(@result, i));
      if i < duckdb_column_count(@result) - 1 then
        Write(', ')
      else
        WriteLn;
    end;
    WriteLn;

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
    WriteLn;

  finally
    // Step 5: Clean up resources
    duckdb_destroy_result(@result);
    duckdb_disconnect(@conn);
    duckdb_close(@db);
  end;
end.
