program DuckFrameFromCSV;

{$mode objfpc}{$H+}{$J-}

uses
  SysUtils, DuckDB.DataFrame;

var
  DF: TDuckFrame;

begin
  try
    // Basic usage - default settings (has headers, comma delimiter)
    DF := TDuckFrame.CreateFromCSV('data.csv');
    try
      WriteLn('Successfully loaded CSV file');
      WriteLn;

      DF.Print;  // Display the data
      WriteLn;

      DF.Describe; // Show summary statistics
      WriteLn;

      DF.UniqueCounts('country').Print; // Show unique counts of country
      WriteLn;

      DF.Info;   // Show structure information
      WriteLn;

    finally
      DF.Free;
    end;

  except
    on E: EDuckDBError do
      WriteLn('DuckDB Error: ', E.Message);
    on E: Exception do
      WriteLn('Error: ', E.Message);
  end;


  WriteLn('Press enter to quit ...');
  ReadLn;
end.
