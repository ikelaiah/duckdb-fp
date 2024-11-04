program DemoWrapper02;

{$mode objfpc}{$H+}

uses
  SysUtils, DuckDB.Wrapper, DuckDB.DataFrame;

var
  DB: TDuckDBConnection;
  DF: TDuckFrame;
begin
  try
    // Create and open connection
    DB := TDuckDBConnection.Create;
    try
      DB.Open();
      
      // Create test data
      DB.ExecuteSQL(
        'CREATE TABLE test(id INTEGER, name VARCHAR);' +
        'INSERT INTO test VALUES ' +
        '(5, ''Small''),' +
        '(15, ''Big''),' +
        '(25, ''Bigger'');'
      );
      
      // Query and display all data
      DF := DB.Query('SELECT * FROM test');
      try
        WriteLn('All data:');
        DF.Print;
        
        // Use SQL filtering instead
        WriteLn('Filtered data (id > 10):');
        DF.Free;
        DF := DB.Query('SELECT * FROM test WHERE id > 10');
        DF.Print;
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
  
  WriteLn('Press Enter to exit...');
  ReadLn;
end.
