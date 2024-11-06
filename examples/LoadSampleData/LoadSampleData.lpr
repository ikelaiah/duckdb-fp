program LoadSampleData;

{$mode objfpc}{$H+}{$J-}

uses
  Classes, SysUtils, DuckDB.DataFrame, DuckDB.SampleData;

procedure ShowDatasetInfo;
var
  Dataset: TSampleDataset;
begin
  WriteLn('Available Sample Datasets:');
  WriteLn('-------------------------');
  for Dataset := Low(TSampleDataset) to High(TSampleDataset) do
  begin
    WriteLn(Format('%d. %s', [Ord(Dataset), TDuckDBSampleData.GetDataInfo(Dataset)]));
    WriteLn;
  end;
end;

procedure AnalyzeMTCars;
var
  DF: TDuckFrame;
  CorrDF: TDuckFrame;
begin
  WriteLn('Analyzing MTCars Dataset:');
  WriteLn('------------------------');

  // Load the dataset
  DF := TDuckDBSampleData.LoadData(sdMtcars);
  try
    // Show basic information
    WriteLn('Dataset Info:');
    DF.Info;
    WriteLn;

    // Show first few rows
    WriteLn('First 5 rows:');
    DF.Head(5).Print;
    WriteLn;

    // Basic statistics
    WriteLn('Summary Statistics:');
    DF.Describe;
    WriteLn;

    // Correlation between MPG and Weight
    WriteLn('Correlation between MPG and Weight:');
    CorrDF := DF.Select(['mpg', 'wt']).CorrPearson;
    try
      // Print the correlation matrix
      WriteLn('Correlation matrix:');
      CorrDF.Print;

      // The correlation matrix is 2x2, and we want the off-diagonal element
      // The correlation matrix will look like this:
      //        mpg     wt
      // mpg    1.0    corr
      // wt     corr   1.0
      WriteLn(FormatFloat('0.0000', CorrDF.ValuesByName[0, 'wt']));  // Get correlation from row 0 (mpg row)
    finally
      CorrDF.Free;
    end;
  finally
    DF.Free;
  end;
end;

procedure AnalyzeIris;
var
  DF: TDuckFrame;
  CountsDF: TDuckFrame;
begin
  WriteLn('Analyzing Iris Dataset:');
  WriteLn('----------------------');

  DF := TDuckDBSampleData.LoadData(sdIris);
  try
    // Show basic statistics
    WriteLn('Basic statistics by measurement:');
    DF.Describe;
    WriteLn;

    // Show species distribution
    WriteLn('Species distribution:');
    CountsDF := DF.UniqueCounts('species');
    try
      CountsDF.Print;
    finally
      CountsDF.Free;
    end;
  finally
    DF.Free;
  end;
end;

procedure AnalyzeTitanic;
var
  DF: TDuckFrame;
  CleanDF: TDuckFrame;
  SurvivalDF: TDuckFrame;
begin
  WriteLn('Analyzing Titanic Dataset:');
  WriteLn('-------------------------');

  DF := TDuckDBSampleData.LoadData(sdTitanic);
  try
    // Show survival distribution
    WriteLn('Survival distribution:');
    SurvivalDF := DF.UniqueCounts('Survived');
    try
      SurvivalDF.Print;
    finally
      SurvivalDF.Free;
    end;

    // Show basic information about missing values
    WriteLn('Missing values summary:');
    DF.Info;
    WriteLn;

    // Show clean data info
    WriteLn('Analysis after dropping missing values:');
    CleanDF := DF.DropNA;
    try
      CleanDF.Info;
    finally
      CleanDF.Free;
    end;
  finally
    DF.Free;
  end;
end;

begin
  try
    // Show all available datasets
    ShowDatasetInfo;
    WriteLn;

    // Analyze specific datasets
    AnalyzeMTCars;
    WriteLn;

    AnalyzeIris;
    WriteLn;

    AnalyzeTitanic;

    WriteLn('Press Enter to exit');
    ReadLn;
  except
    on E: Exception do
    begin
      WriteLn('Error: ', E.Message);
      ReadLn;
    end;
  end;
end.
