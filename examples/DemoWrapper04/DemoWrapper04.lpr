program DemoWrapper04;

{$mode objfpc}{$H+}{$J-}

uses
  SysUtils,
  DuckDB.Wrapper,
  DuckDB.DataFrame;

var
  DB: TDuckDBConnection;
  DF, ResultDF: TDuckFrame;
begin
  try
    // Initialize DuckDB
    DB := TDuckDBConnection.Create;
    try
      DB.Open;

      // Create a sample DataFrame with some data
      DF := DB.Query(
        'SELECT ' +
        '  CASE WHEN x < 4 THEN x ELSE NULL END as col1, ' +
        '  x % 3 as col2, ' +
        '  x / 2.0 as col3 ' +
        'FROM generate_series(0, 9) as t(x)');
      try
        WriteLn('Original DataFrame:');
        WriteLn('----------------');
        DF.Print;
        WriteLn;

        // Show unique counts for col2
        WriteLn('Unique counts for col2:');
        WriteLn('----------------');
        ResultDF := DF.UniqueCounts('col2');
        try
          ResultDF.Print;
        finally
          ResultDF.Free;
        end;
        WriteLn;

        // Show histogram for col3
        WriteLn('Histogram for col3:');
        WriteLn('----------------');
        DF.PlotHistogram('col3', 4);
        WriteLn;

        // Show correlation matrix
        WriteLn('Correlation Matrix (Pearson):');
        WriteLn('----------------');
        ResultDF := DF.CorrPearson;
        try
          ResultDF.Print;
        finally
          ResultDF.Free;
        end;
        WriteLn;


        // Show correlation matrix
        WriteLn('Correlation Matrix (Spearman):');
        WriteLn('----------------');
        ResultDF := DF.CorrSpearman;
        try
          ResultDF.Print;
        finally
          ResultDF.Free;
        end;
        WriteLn;

        // Demonstrate missing data handling
        WriteLn('DataFrame after dropping NULL values:');
        WriteLn('----------------');
        ResultDF := DF.DropNA;
        try
          ResultDF.Print;
        finally
          ResultDF.Free;
        end;
        WriteLn;

        WriteLn('DataFrame after filling NULL values with -1:');
        WriteLn('----------------');
        ResultDF := DF.FillNA(-1);
        try
          ResultDF.Print;
        finally
          ResultDF.Free;
        end;

      finally
        DF.Free;
      end;

    finally
      DB.Free;
    end;

  except
    on E: Exception do
      WriteLn(E.ClassName, ': ', E.Message);
  end;

  WriteLn('Press Enter to exit...');
  ReadLn;
end.
