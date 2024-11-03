program DemoWrapper01;

{$mode objfpc}{$H+}{$J-}

uses
  SysUtils, DuckDB.Wrapper, DuckDB.DataFrame;

var
  db: TDuckDBConnection;
  df: TDuckFrame;
  headDF: TDuckFrame;
  index: integer;

begin
  try
    // Create and open connection (in-memory database)
    db := TDuckDBConnection.Create;
    try
      db.Open();
      
      // Create table and insert data
      db.ExecuteSQL(
        'CREATE TABLE test(id INTEGER, name VARCHAR, value DOUBLE);' +
        'INSERT INTO test VALUES ' +
        '(1, ''Alice'', 10.5),' +
        '(2, ''Bob'', 20.7),' +
        '(3, ''Charlie'', 15.3);'
      );
      
      // Query data into DataFrame
      df := db.Query('SELECT * FROM test');
      try
        // Print all data
        WriteLn('All data:');
        df.Print;
        
        // Save to CSV
        df.SaveToCSV('test.csv'); 

        // Get first few rows from this query
        headDF := df.Head(2);
        try
          WriteLn('First 2 rows:');
          headDF.Print;
        finally
          headDF.Free;
        end;
        
        // Access by column name
        WriteLn('Names in column `name`:');
        for index := 0 to df.RowCount - 1 do
          WriteLn(df.ValuesByName[index, 'name']);
        
      finally
        df.Free;
      end;
      
    finally
      db.Free;
    end;
    
  except
    on E: Exception do
      WriteLn('Error: ', E.Message);
  end;
  
  // Wait for user input before exiting
  WriteLn('Press Enter to exit...');
  ReadLn;
end.
