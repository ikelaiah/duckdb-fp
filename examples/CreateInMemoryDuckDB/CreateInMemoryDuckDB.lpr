program CreateInMemoryDuckDB;

{$mode objfpc}{$H+}{$J-}

uses
  SysUtils, DuckDB.Wrapper, DuckDB.DataFrame;

var
  DB: TDuckDBConnection;
  DF, HeadDf: TDuckFrame;
  Index: integer;

begin
  try
    // Create and open connection (in-memory database)
    DB := TDuckDBConnection.Create;
    try
      DB.Open();
      
      // Create table and insert data
      DB.ExecuteSQL(
        'CREATE TABLE test(id INTEGER, name VARCHAR, value DOUBLE);' +
        'INSERT INTO test VALUES ' +
        '(1, ''Alice'', 10.5),' +
        '(2, ''Bob'', 20.7),' +
        '(3, ''Charlie'', 15.3);'
      );
      
      // Query data into DataFrame
      DF := DB.Query('SELECT * FROM test');
      try
        // Print all data
        WriteLn('All data:');
        DF.Print;
        WriteLn;

        // Save to CSV
        DF.SaveToCSV('test.csv');

        // Get first few rows from this query
        HeadDF := DF.Head(2);
        try
          WriteLn('First 2 rows:');
          HeadDF.Print;
        finally
          HeadDF.Free;
        end;
        WriteLn;

        // Access by column name
        WriteLn('Names in column `name`:');
        for Index := 0 to DF.RowCount - 1 do
          WriteLn(DF.ValuesByName[Index, 'name']);
        WriteLn;

      finally
        DF.Free;
      end;
      
    finally
      DB.Free;
    end;
    
  except
    on E: Exception do
      WriteLn('Error: ', E.Message);
  end;
  
  // Wait for user input before exiting
  WriteLn('Press Enter to exit...');
  ReadLn;
end.
