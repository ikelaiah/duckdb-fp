program DuckFrameFromBlank;

{$mode objfpc}{$H+}{$J-}

uses
  Classes,
  SysUtils,
  DuckDB.DataFrame;

var
  DuckFrame: TDuckFrame;

begin
  // Method 1: Create blank with predefined columns
  WriteLn('Method 1: Create blank with predefined columns');
  DuckFrame := TDuckFrame.CreateBlank(['Name', 'Age', 'City'],
                                      [dctString, dctInteger, dctString]);
  try
    // Adding two rows to this dataframe
    DuckFrame.AddRow(['John', 25, 'New York']);
    DuckFrame.AddRow(['Alice', 30, 'London']);

    // Print data frame
    DuckFrame.Print;
  finally
    DuckFrame.Free;
  end;

  // Method 2: Create blank and add columns manually
  WriteLn;
  WriteLn('Method 2: Create blank and add columns manually');
  DuckFrame := TDuckFrame.Create;
  try
    // Create column definition and add the first row
    DuckFrame.AddColumn('Name', dctString);
    DuckFrame.AddColumn('Age', dctInteger);
    DuckFrame.AddColumn('City', dctString);
    DuckFrame.AddRow(['John', 25, 'New York']);

    // Or set values individually for the second row:
    DuckFrame.AddRow([Null, Null, Null]);
    DuckFrame.SetValue(1, 'Name', 'Alice');
    DuckFrame.SetValue(1, 'Age', 30);
    DuckFrame.SetValue(1, 'City', 'London');

    // Print data frame
    DuckFrame.Print;
  finally
    DuckFrame.Free;
  end;

  WriteLn('Press Enter to quit...');
  ReadLn;
end.
