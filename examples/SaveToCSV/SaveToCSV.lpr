program TestCSV;

{$mode objfpc}{$H+}{$J-}

uses
  SysUtils, DuckDB.DataFrame, DuckDB.Wrapper;

var
  DF: TDuckFrame;
  DB: TDuckDBConnection;
  SQL: string;
begin
  DB := TDuckDBConnection.Create;
  try
    DB.Open;

    // Create the SQL query with proper line concatenation
    SQL := 'SELECT ' +
           '1 as id, ' +
           '''Simple text'' as col1, ' +
           '''Text with' + #13#10 +
           'multiple' + #13#10 +
           'lines'' as col2, ' +
           '''Text with,comma'' as col3, ' +
           '''Text with "quotes"'' as col4';

    // Create a test DataFrame with multi-line content
    DF := DB.Query(SQL);
    try
      // Save to CSV
      DF.SaveToCSV('test_multiline.csv');
      WriteLn('CSV file created successfully!');
    finally
      DF.Free;
    end;

  finally
    DB.Free;
  end;

  WriteLn('Press Enter to exit...');
  ReadLn;
end.
