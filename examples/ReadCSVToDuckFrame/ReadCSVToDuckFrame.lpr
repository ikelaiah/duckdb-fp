program ReadCSVToDuckFrame;

{$mode objfpc}{$H+}{$J-}

uses
  SysUtils,
  DuckDB.Wrapper,
  DuckDB.DataFrame;

var
  DF: TDuckFrame;
  FilePath: string;

begin
  FilePath := 'RandomData.csv';
  DF := TDuckDBConnection.ReadCSV(FilePath);
  try
    // Print the DataFrame
    DF.Print;
    // Print the DataFrame description
    DF.Describe;
  finally
    DF.Free;
  end;

  WriteLn('Press Enter to exit...');
  ReadLn;
end.
