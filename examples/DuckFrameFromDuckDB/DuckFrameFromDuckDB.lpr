program DuckFrameFromDuckDB;

{$mode objfpc}{$H+}{$J-}

uses
  Classes,
  SysUtils,
  DuckDB.DataFrame,
  DuckDB.Wrapper;

  procedure CreateSampleDatabase;
  var
    DB: TDuckDBConnection;
    DataDir: string;
  begin
    try
      // Ensure data directory exists
      DataDir := 'data';
      if not DirectoryExists(DataDir) then
      begin
        try
          CreateDir(DataDir);
        except
          on E: Exception do
            raise EDuckDBError.CreateFmt('Failed to create directory: %s', [E.Message]);
        end;
      end;

      // Create a new database connection
      DB := TDuckDBConnection.Create;
      try
        DB.Open(DataDir + DirectorySeparator + 'customers.db');

        // Drop table if it exists
        DB.ExecuteSQL('DROP TABLE IF EXISTS customers');

        // Create a sample table
        DB.ExecuteSQL(
          'CREATE TABLE customers (' +
          '  id INTEGER PRIMARY KEY,' +
          '  name VARCHAR,' +
          '  age INTEGER,' +
          '  country VARCHAR,' +
          '  signup_date DATE,' +
          '  total_purchases DECIMAL(10,2)' +
          ')'
        );

        // Insert some sample data
        DB.ExecuteSQL(
          'INSERT INTO customers VALUES' +
          '(1, ''John Smith'', 35, ''USA'', ''2023-01-15'', 1250.50),' +
          '(2, ''Maria Garcia'', 28, ''Spain'', ''2023-02-20'', 890.25),' +
          '(3, ''Yuki Tanaka'', 42, ''Japan'', ''2023-01-10'', 3200.75),' +
          '(4, ''Hans Mueller'', 31, ''Germany'', ''2023-03-05'', 750.00),' +
          '(5, ''Sophie Martin'', 49, ''France'', ''2023-02-01'', 1875.30)'
        );

      finally
        DB.Free;
      end;

    except
      on E: Exception do
      begin
        WriteLn('Error in CreateSampleDatabase: ', E.Message);
      end;
    end;
  end;

procedure CreateDuckFrameFromDB;
var
  Frame: TDuckFrame;
  ColNames: TStringArray;
  I: Integer;
  DataDir: string;
begin
  try
    // First create our sample database
    CreateSampleDatabase;

    // Now load it into a DuckFrame
    DataDir := 'data';
    Frame := TDuckFrame.CreateFromDuckDB(DataDir + DirectorySeparator + 'customers.db', 'customers');
    try
      WriteLn('=== Sample Customer Data ===');
      WriteLn(Format('Loaded %d rows with %d columns', [Frame.RowCount, Frame.ColumnCount]));

      // Get and display column names
      ColNames := Frame.GetColumnNames;
      Write('Column names: ');
      for I := 0 to High(ColNames) do
      begin
        Write(ColNames[I]);
        if I < High(ColNames) then
          Write(', ');
      end;
      WriteLn;
      WriteLn;

      // Print the data
      WriteLn('Data:');
      Frame.Print;  // Add this to see the actual data
      WriteLn;

      // Basic statistics for numeric columns
      WriteLn('=== Basic Statistics for Age ===');
      if Frame.FindColumnIndex('age') >= 0 then
      begin
        WriteLn('Histogram of Age Distribution:');
        Frame.PlotHistogram('age', 5);  // Show age distribution in 5 bins
      end;
      WriteLn;

      // Show unique counts for a categorical column
      WriteLn('=== Country Distribution ===');
      if Frame.FindColumnIndex('country') >= 0 then
      begin
        Frame.UniqueCounts('country').Print;
      end;
      WriteLn;

      // Demonstrate handling missing data
      WriteLn('=== Handling Missing Data ===');
      WriteLn('Rows with complete data: ', Frame.DropNA.RowCount);

    finally
      Frame.Free;
    end;

  except
    on E: Exception do
    begin
      WriteLn('Error: ', E.Message);
      WriteLn('Stack trace:');
      WriteLn(BackTraceStrFunc(ExceptAddr));
    end;
  end;
end;

begin
  try
    CreateDuckFrameFromDB;
  except
    on E: Exception do
      WriteLn('Error: ', E.Message);
  end;

  WriteLn;
  WriteLn('Press Enter to quit...');
  ReadLn;
end.
